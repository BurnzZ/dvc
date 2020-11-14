import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
from multiprocessing import cpu_count
from urllib.parse import urlparse

from funcy import cached_property, decorator

from dvc.dir_info import DirInfo
from dvc.hash_info import HashInfo
from dvc.path_info import PosixPathInfo
from dvc.exceptions import DvcException, DvcIgnoreInCollectedDirError
from dvc.ignore import DvcIgnore
from dvc.path_info import URLInfo
from dvc.progress import Tqdm
from dvc.state import StateNoop
from dvc.utils import tmp_fname
from dvc.utils.fs import makedirs, move
from dvc.utils.http import open_url

logger = logging.getLogger(__name__)


class RemoteCmdError(DvcException):
    def __init__(self, remote, cmd, ret, err):
        super().__init__(
            "{remote} command '{cmd}' finished with non-zero return code"
            " {ret}': {err}".format(remote=remote, cmd=cmd, ret=ret, err=err)
        )


class RemoteActionNotImplemented(DvcException):
    def __init__(self, action, scheme):
        m = f"{action} is not supported for {scheme} remotes"
        super().__init__(m)


class RemoteMissingDepsError(DvcException):
    pass


@decorator
def use_state(call):
    tree = call._args[0]  # pylint: disable=protected-access
    with tree.state:
        return call()


class BaseTree:
    scheme = "base"
    REQUIRES = {}
    PATH_CLS = URLInfo
    JOBS = 4 * cpu_count()

    CHECKSUM_DIR_SUFFIX = ".dir"
    HASH_JOBS = max(1, min(4, cpu_count() // 2))
    DEFAULT_VERIFY = False
    LIST_OBJECT_PAGE_SIZE = 1000
    TRAVERSE_WEIGHT_MULTIPLIER = 5
    TRAVERSE_PREFIX_LEN = 3
    TRAVERSE_THRESHOLD_SIZE = 500000
    CAN_TRAVERSE = True

    CACHE_MODE = None
    SHARED_MODE_MAP = {None: (None, None), "group": (None, None)}
    PARAM_CHECKSUM = None

    state = StateNoop()

    def __init__(self, repo, config):
        self.repo = repo
        self.config = config

        self._check_requires(config)

        shared = config.get("shared")
        self._file_mode, self._dir_mode = self.SHARED_MODE_MAP[shared]

        self.verify = config.get("verify", self.DEFAULT_VERIFY)
        self.path_info = None
        self.vdir = None

    @cached_property
    def hash_jobs(self):
        return (
            self.config.get("checksum_jobs")
            or (self.repo and self.repo.config["core"].get("checksum_jobs"))
            or self.HASH_JOBS
        )

    @classmethod
    def get_missing_deps(cls):
        import importlib

        missing = []
        for package, module in cls.REQUIRES.items():
            try:
                importlib.import_module(module)
            except ImportError:
                missing.append(package)

        return missing

    def _check_requires(self, config):
        missing = self.get_missing_deps()
        if not missing:
            return

        url = config.get("url", f"{self.scheme}://")
        msg = (
            "URL '{}' is supported but requires these missing "
            "dependencies: {}. If you have installed dvc using pip, "
            "choose one of these options to proceed: \n"
            "\n"
            "    1) Install specific missing dependencies:\n"
            "        pip install {}\n"
            "    2) Install dvc package that includes those missing "
            "dependencies: \n"
            "        pip install 'dvc[{}]'\n"
            "    3) Install dvc package with all possible "
            "dependencies included: \n"
            "        pip install 'dvc[all]'\n"
            "\n"
            "If you have installed dvc from a binary package and you "
            "are still seeing this message, please report it to us "
            "using https://github.com/iterative/dvc/issues. Thank you!"
        ).format(url, missing, " ".join(missing), self.scheme)
        raise RemoteMissingDepsError(msg)

    @classmethod
    def supported(cls, config):
        if isinstance(config, (str, bytes)):
            url = config
        else:
            url = config["url"]

        # NOTE: silently skipping remote, calling code should handle that
        parsed = urlparse(url)
        return parsed.scheme == cls.scheme

    @property
    def file_mode(self):
        return self._file_mode

    @property
    def dir_mode(self):
        return self._dir_mode

    @property
    def cache(self):
        return getattr(self.repo.cache, self.scheme)

    def open(self, path_info, mode="r", encoding=None):
        if hasattr(self, "_generate_download_url"):
            func = self._generate_download_url  # noqa,pylint:disable=no-member
            get_url = partial(func, path_info)
            return open_url(get_url, mode=mode, encoding=encoding)

        raise RemoteActionNotImplemented("open", self.scheme)

    def exists(self, path_info, use_dvcignore=True):
        raise NotImplementedError

    # pylint: disable=unused-argument

    def isdir(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return False

    def isfile(self, path_info):
        """Optional: Overwrite only if the remote has a way to distinguish
        between a directory and a file.
        """
        return True

    def iscopy(self, path_info):
        """Check if this file is an independent copy."""
        return False  # We can't be sure by default

    def walk_files(self, path_info, **kwargs):
        """Return a generator with `PathInfo`s to all the files.

        Optional kwargs:
            prefix (bool): If true `path_info` will be treated as a prefix
                rather than directory path.
        """
        raise NotImplementedError

    def is_empty(self, path_info):
        return False

    def remove(self, path_info):
        raise RemoteActionNotImplemented("remove", self.scheme)

    def makedirs(self, path_info):
        """Optional: Implement only if the remote needs to create
        directories before copying/linking/moving data
        """

    def move(self, from_info, to_info, mode=None):
        assert mode is None
        self.copy(from_info, to_info)
        self.remove(from_info)

    def copy(self, from_info, to_info):
        raise RemoteActionNotImplemented("copy", self.scheme)

    def copy_fobj(self, fobj, to_info):
        raise RemoteActionNotImplemented("copy_fobj", self.scheme)

    def symlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("symlink", self.scheme)

    def hardlink(self, from_info, to_info):
        raise RemoteActionNotImplemented("hardlink", self.scheme)

    def reflink(self, from_info, to_info):
        raise RemoteActionNotImplemented("reflink", self.scheme)

    @staticmethod
    def protect(path_info):
        pass

    def is_protected(self, path_info):
        return False

    # pylint: enable=unused-argument

    @staticmethod
    def unprotect(path_info):
        pass

    @classmethod
    def is_dir_hash(cls, hash_):
        if not hash_:
            return False
        return hash_.endswith(cls.CHECKSUM_DIR_SUFFIX)

    @use_state
    def get_hash(self, path_info, **kwargs):
        assert path_info and (
            isinstance(path_info, str) or path_info.scheme == self.scheme
        )

        if not self.exists(path_info):
            return None

        # pylint: disable=assignment-from-none
        hash_info = self.state.get(path_info)

        # If we have dir hash in state db, but dir cache file is lost,
        # then we need to recollect the dir via .get_dir_hash() call below,
        # see https://github.com/iterative/dvc/issues/2219 for context
        if (
            hash_info
            and hash_info.isdir
            and not self.cache.tree.exists(
                self.cache.tree.hash_to_path_info(hash_info.value)
            )
        ):
            hash_info = None

        if hash_info:
            assert hash_info.name == self.PARAM_CHECKSUM
            if hash_info.isdir:
                self.cache.set_dir_info(hash_info)
            return hash_info

        if self.isdir(path_info):
            hash_info = self.get_dir_hash(path_info, **kwargs)
        else:
            hash_info = self.get_file_hash(path_info)

        if hash_info and self.exists(path_info):
            self.state.save(path_info, hash_info)

        return hash_info

    def get_file_hash(self, path_info):
        raise NotImplementedError

    def hash_to_path_info(self, hash_):
        return self.path_info / hash_[0:2] / hash_[2:]

    def _calculate_hashes(self, file_infos):
        file_infos = list(file_infos)
        with Tqdm(
            total=len(file_infos),
            unit="md5",
            desc="Computing file/dir hashes (only done once)",
        ) as pbar:
            worker = pbar.wrap_fn(self.get_file_hash)
            with ThreadPoolExecutor(max_workers=self.hash_jobs) as executor:
                hash_infos = executor.map(worker, file_infos)
                return dict(zip(file_infos, hash_infos))

    def _collect_dir(self, path_info, **kwargs):

        file_infos = set()

        # - stage.reload().outs
        #   - [LocalOutput: 'rpm-images']

        # - hash_info = stage.reload().outs[0].hash_info
        #   - HashInfo(name='md5', value='ca33682bbdb304025242bbc1b0d79d52.dir', dir_info=None, size=58385, nfiles=24)
        #       - If I could attach this somehow to the vdir.
        #       - then I won't need stage, only the 'vdir'

        # - dir_info = self.cache.load_dir_cache(hash_info)
        #   - <dvc.dir_info.DirInfo object at 0x103e247c0>

        # - dir_info.to_list()
        # [{'md5': '194577a7e20bdcc7afbb718f502c134c', 'relpath': '.DS_Store'}, {'md5': '6f398d502ec50b2674d329130221d822', 'relpath': 'ProblemList.txt'}, {'md5': 'f77412af6a7bb0153be6f90077e7e2eb', 'relpath': 'problem-E-01/1.png'}, {'md5': 'a0b35192e8583255737bd790bb545704', 'relpath': 'problem-E-01/2.png'}, {'md5': '2ccdba5ed29208a802fba9147c554b84', 'relpath': 'problem-E-01/3.png'}, {'md5': 'ddd8cf838688e3bb9fa8ffba233f2bbc', 'relpath': 'problem-E-01/4.png'}, {'md5': '480b7470db43097e3cf78ee024f6f101', 'relpath': 'problem-E-01/5.png'}, {'md5': '64c0d57bae50d2a8effbb29abbdd1093', 'relpath': 'problem-E-01/6.png'}, {'md5': '9cd4266918da92c98d1947b4aac643bb', 'relpath': 'problem-E-01/7.png'}, {'md5': '7bbc0fbc545fd6e603f20c1c3faf5caa', 'relpath': 'problem-E-01/8.png'}, {'md5': '82f79c9899f212ad624162c3c84e4523', 'relpath': 'problem-E-01/A.png'}, {'md5': 'bb66147bfdaf5f4e2e8a31d673166b8c', 'relpath': 'problem-E-01/B.png'}, {'md5': '2e649c878bae8ace221b15354a617d6b', 'relpath': 'problem-E-01/Basic Problem E-01.PNG'}, {'md5': 'ccc9c569c143f9825a667d87c0ccc37e', 'relpath': 'problem-E-01/C-copy.png'}, {'md5': 'b65f54c53e08702f3e52689e3e79d923', 'relpath': 'problem-E-01/C.png'}, {'md5': '3121fb71be8dc32a28182106fd1b7783', 'relpath': 'problem-E-01/D-copy.png'}, {'md5': '7bbc0fbc545fd6e603f20c1c3faf5caa', 'relpath': 'problem-E-01/D.png'}, {'md5': '36c2b50bb5c9eb586641857fa28c6848', 'relpath': 'problem-E-01/E.png'}, {'md5': 'bccb8fa421d026b7f705b3834c11064f', 'relpath': 'problem-E-01/F.png'}, {'md5': '845fda247e14c6c06226b7fc38ff860e', 'relpath': 'problem-E-01/G.png'}, {'md5': '2a6d83e852c48423eccaccdd140bba5e', 'relpath': 'problem-E-01/H-copy.png'}, {'md5': '222bc681fdd1bac4f6ab58a1678da38e', 'relpath': 'problem-E-01/H.png'}, {'md5': 'b026324c6904b2a9cb4b88d6d61c81d1', 'relpath': 'problem-E-01/ProblemAnswer.txt'}, {'md5': '883b7130fff6975a2c7a7960137d1c44', 'relpath': 'problem-E-01/ProblemData.txt'}]
        #   - note that the new file is not contained here
        #   - this is the original unchanged one.

        # The dir_info could then basically become the `file_infos`
        #   - but need to add the 'rpm-images' prefix
        #       - this is easy: `path_info / '.DS_Store'` == PosixPathInfo: 'rpm-images/.DS_Store'

        for fname in self.walk_files(path_info, **kwargs):
            if DvcIgnore.DVCIGNORE_FILE == fname.name:
                raise DvcIgnoreInCollectedDirError(fname.parent)

            file_infos.add(fname)

        # file_infos
        # {PosixPathInfo: 'rpm-images/problem-E-01/C.png', PosixPathInfo: 'rpm-images/problem-E-01/Basic Problem E-01.PNG', PosixPathInfo: 'rpm-images/problem-E-01/F.png', PosixPathInfo: 'rpm-images/problem-E-01/D.png', PosixPathInfo: 'rpm-images/problem-E-01/G.png', PosixPathInfo: 'rpm-images/ProblemList.txt', PosixPathInfo: 'rpm-images/problem-E-01/7.png', PosixPathInfo: 'rpm-images/problem-E-01/ProblemAnswer.txt', PosixPathInfo: 'rpm-images/problem-E-01/B-new.png', PosixPathInfo: 'rpm-images/problem-E-01/2.png', PosixPathInfo: 'rpm-images/problem-E-01/H-copy.png', PosixPathInfo: 'rpm-images/problem-E-01/6.png', PosixPathInfo: 'rpm-images/problem-E-01/5.png', PosixPathInfo: 'rpm-images/problem-E-01/B.png', PosixPathInfo: 'rpm-images/problem-E-01/A.png', PosixPathInfo: 'rpm-images/.DS_Store', PosixPathInfo: 'rpm-images/problem-E-01/E.png', PosixPathInfo: 'rpm-images/problem-E-01/4.png', PosixPathInfo: 'rpm-images/problem-E-01/C-copy.png', PosixPathInfo: 'rpm-images/problem-E-01/1.png', PosixPathInfo: 'rpm-images/problem-E-01/3.png', PosixPathInfo: 'rpm-images/problem-E-01/D-copy.png', PosixPathInfo: 'rpm-images/problem-E-01/8.png', PosixPathInfo: 'rpm-images/problem-E-01/H.png', PosixPathInfo: 'rpm-images/problem-E-01/ProblemData.txt'}

        hash_infos = {fi: self.state.get(fi) for fi in file_infos}
        # {PosixPathInfo: 'rpm-images/problem-E-01/C.png': HashInfo(name='md5', value='b65f54c53e08702f3e52689e3e79d923', dir_info=None, size=1365, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/Basic Problem E-01.PNG': HashInfo(name='md5', value='2e649c878bae8ace221b15354a617d6b', dir_info=None, size=22477, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/F.png': HashInfo(name='md5', value='bccb8fa421d026b7f705b3834c11064f', dir_info=None, size=1383, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/D.png': HashInfo(name='md5', value='7bbc0fbc545fd6e603f20c1c3faf5caa', dir_info=None, size=986, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/G.png': HashInfo(name='md5', value='845fda247e14c6c06226b7fc38ff860e', dir_info=None, size=1328, nfiles=None), PosixPathInfo: 'rpm-images/ProblemList.txt': HashInfo(name='md5', value='6f398d502ec50b2674d329130221d822', dir_info=None, size=18, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/7.png': HashInfo(name='md5', value='9cd4266918da92c98d1947b4aac643bb', dir_info=None, size=1525, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/ProblemAnswer.txt': HashInfo(name='md5', value='b026324c6904b2a9cb4b88d6d61c81d1', dir_info=None, size=3, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/B-new.png': None, PosixPathInfo: 'rpm-images/problem-E-01/2.png': HashInfo(name='md5', value='a0b35192e8583255737bd790bb545704', dir_info=None, size=1192, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/H-copy.png': HashInfo(name='md5', value='2a6d83e852c48423eccaccdd140bba5e', dir_info=None, size=2643, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/6.png': HashInfo(name='md5', value='64c0d57bae50d2a8effbb29abbdd1093', dir_info=None, size=2552, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/5.png': HashInfo(name='md5', value='480b7470db43097e3cf78ee024f6f101', dir_info=None, size=1281, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/B.png': HashInfo(name='md5', value='bb66147bfdaf5f4e2e8a31d673166b8c', dir_info=None, size=960, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/A.png': HashInfo(name='md5', value='82f79c9899f212ad624162c3c84e4523', dir_info=None, size=866, nfiles=None), PosixPathInfo: 'rpm-images/.DS_Store': HashInfo(name='md5', value='194577a7e20bdcc7afbb718f502c134c', dir_info=None, size=6148, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/E.png': HashInfo(name='md5', value='36c2b50bb5c9eb586641857fa28c6848', dir_info=None, size=1039, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/4.png': HashInfo(name='md5', value='ddd8cf838688e3bb9fa8ffba233f2bbc', dir_info=None, size=1240, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/C-copy.png': HashInfo(name='md5', value='ccc9c569c143f9825a667d87c0ccc37e', dir_info=None, size=3081, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/1.png': HashInfo(name='md5', value='f77412af6a7bb0153be6f90077e7e2eb', dir_info=None, size=1829, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/3.png': HashInfo(name='md5', value='2ccdba5ed29208a802fba9147c554b84', dir_info=None, size=1371, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/D-copy.png': HashInfo(name='md5', value='3121fb71be8dc32a28182106fd1b7783', dir_info=None, size=2812, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/8.png': HashInfo(name='md5', value='7bbc0fbc545fd6e603f20c1c3faf5caa', dir_info=None, size=986, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/H.png': HashInfo(name='md5', value='222bc681fdd1bac4f6ab58a1678da38e', dir_info=None, size=1282, nfiles=None), PosixPathInfo: 'rpm-images/problem-E-01/ProblemData.txt': HashInfo(name='md5', value='883b7130fff6975a2c7a7960137d1c44', dir_info=None, size=18, nfiles=None)}

        # NOTES: The `hash_infos` basically have the md5 values added on top of
        # the `file_infos`.
        #   - with the 'size' data

        # TODO: refactor

        if self.vdir:
            vhash_infos = {}
            for fname in self.cache.load_dir_cache(self.vdir.hash_info).to_list():
                fi = path_info / fname['relpath']
                hi = HashInfo('md5', fname['md5'])  # no size is saved in the cache
                vhash_infos[fi] = hi

            # This contains the new file
            if self.vdir.operation == 'cp':
                hash_infos = {**hash_infos, **vhash_infos}

        not_in_state = {fi for fi, hi in hash_infos.items() if hi is None}
        # {PosixPathInfo: 'rpm-images/problem-E-01/B-new.png'}

        new_hash_infos = self._calculate_hashes(not_in_state)
        # {PosixPathInfo: 'rpm-images/problem-E-01/B-new.png': HashInfo(name='md5', value='01c857535d2e69aea214d7793d7d376d', dir_info=None, size=3469, nfiles=None)}

        hash_infos.update(new_hash_infos)

        dir_info = DirInfo(self.vdir)
        for fi, hi in hash_infos.items():
            # fi
            # PosixPathInfo: 'rpm-images/problem-E-01/C.png'

            # hi
            # HashInfo(name='md5', value='b65f54c53e08702f3e52689e3e79d923', dir_info=None, size=1365, nfiles=None)

            # NOTE: this is lossy transformation:
            #   "hey\there" -> "hey/there"
            #   "hey/there" -> "hey/there"
            # The latter is fine filename on Windows, which
            # will transform to dir/file on back transform.
            #
            # Yes, this is a BUG, as long as we permit "/" in
            # filenames on Windows and "\" on Unix
            dir_info.trie[fi.relative_to(path_info).parts] = hi

            # fi.relative_to(path_info).parts
            # ('problem-E-01', 'C.png')

            # after one iteration, this is what `dir_info.trie` looks like
            # Trie([(('problem-E-01', 'C.png'), HashInfo(name='md5', value='b65f54c53e08702f3e52689e3e79d923', dir_info=None, size=1365, nfiles=None))])

        return dir_info

    @use_state
    def get_dir_hash(self, path_info, **kwargs):
        dir_info = self._collect_dir(path_info, **kwargs)
        hash_info = self.repo.cache.local.save_dir_info(dir_info)

        # orig
        # HashInfo(name='md5', value='e1a0cf7fcebe1c12bc0adeaf7ca38dfd.dir', dir_info=<dvc.dir_info.DirInfo object at 0x1100c4460>, size=1996, nfiles=25)

        # new behavior
        # HashInfo(name='md5', value='e1a0cf7fcebe1c12bc0adeaf7ca38dfd.dir', dir_info=<dvc.dir_info.DirInfo object at 0x121ea2670>, size=None, nfiles=25)

        hash_info.size = dir_info.size

        # orig
        # dir_info.size = 61854

        # new behavior
        # dir_info.size = 61854

        self.vdir.hash_info = hash_info  # pass it back so can be accessed by stage

        return hash_info

    def upload(self, from_info, to_info, name=None, no_progress_bar=False):
        if not hasattr(self, "_upload"):
            raise RemoteActionNotImplemented("upload", self.scheme)

        if to_info.scheme != self.scheme:
            raise NotImplementedError

        if from_info.scheme != "local":
            raise NotImplementedError

        logger.debug("Uploading '%s' to '%s'", from_info, to_info)

        name = name or from_info.name

        self._upload(  # noqa, pylint: disable=no-member
            from_info.fspath,
            to_info,
            name=name,
            no_progress_bar=no_progress_bar,
        )

    def download(
        self,
        from_info,
        to_info,
        name=None,
        no_progress_bar=False,
        file_mode=None,
        dir_mode=None,
    ):
        if not hasattr(self, "_download"):
            raise RemoteActionNotImplemented("download", self.scheme)

        if from_info.scheme != self.scheme:
            raise NotImplementedError

        if to_info.scheme == self.scheme != "local":
            self.copy(from_info, to_info)
            return 0

        if to_info.scheme != "local":
            raise NotImplementedError

        if self.isdir(from_info):
            return self._download_dir(
                from_info, to_info, name, no_progress_bar, file_mode, dir_mode
            )
        return self._download_file(
            from_info, to_info, name, no_progress_bar, file_mode, dir_mode
        )

    def _download_dir(
        self, from_info, to_info, name, no_progress_bar, file_mode, dir_mode
    ):
        from_infos = list(self.walk_files(from_info))
        to_infos = (
            to_info / info.relative_to(from_info) for info in from_infos
        )

        with Tqdm(
            total=len(from_infos),
            desc="Downloading directory",
            unit="Files",
            disable=no_progress_bar,
        ) as pbar:
            download_files = pbar.wrap_fn(
                partial(
                    self._download_file,
                    name=name,
                    no_progress_bar=True,
                    file_mode=file_mode,
                    dir_mode=dir_mode,
                )
            )
            with ThreadPoolExecutor(max_workers=self.JOBS) as executor:
                futures = [
                    executor.submit(download_files, from_info, to_info)
                    for from_info, to_info in zip(from_infos, to_infos)
                ]

                # NOTE: unlike pulling/fetching cache, where we need to
                # download everything we can, not raising an error here might
                # turn very ugly, as the user might think that he has
                # downloaded a complete directory, while having a partial one,
                # which might cause unexpected results in his pipeline.
                for future in as_completed(futures):
                    # NOTE: executor won't let us raise until all futures that
                    # it has are finished, so we need to cancel them ourselves
                    # before re-raising.
                    exc = future.exception()
                    if exc:
                        for entry in futures:
                            entry.cancel()
                        raise exc

    def _download_file(
        self, from_info, to_info, name, no_progress_bar, file_mode, dir_mode
    ):
        makedirs(to_info.parent, exist_ok=True, mode=dir_mode)

        logger.debug("Downloading '%s' to '%s'", from_info, to_info)
        name = name or to_info.name

        tmp_file = tmp_fname(to_info)

        self._download(  # noqa, pylint: disable=no-member
            from_info, tmp_file, name=name, no_progress_bar=no_progress_bar
        )

        move(tmp_file, to_info, mode=file_mode)
