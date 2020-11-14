from pathlib import Path
from shutil import copy
from dataclasses import dataclass
from dvc.repo.add import add
from dvc.hash_info import HashInfo


# TODO: move to separate module later
@dataclass
class VirtualDir:
    operation: str  # e.g. cp, mv, rm
    src: str
    dst: str
    rm_paths: list

    local_src: bool = True  # only for cp
    hash_info: HashInfo = None


def cp(repo, src=None, dst=None, local_src=False):

    if local_src:
        copy(src, dst)

    # TODO: need to validate that dst is a tracked *.dvc file

    target = 'rpm-images'  # TODO: hard coded for now, but derive this later

    vdir = VirtualDir(operation='cp', src=src, dst=dst, rm_paths=None, local_src=local_src)

    return add(repo, [target], vdir=vdir)  # FIXME: targets





    # pairs = repo.collect_granular(target, with_deps=with_deps, recursive=recursive)

    # stage = pairs[0][0]  # FIXME: figure out a way to derive this

    # used_cache = stage.get_used_cache()

    # # TODO: checksum calculation? - check `dvc add`

    # # TODO: does using scheme=local able to push it later?
    # # TODO: name?

    # used_cache.add('local', checksum, name)

    # return ""  # TODO: what to return??
