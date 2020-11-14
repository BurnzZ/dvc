import logging
import os

import colorama

from dvc.dvcfile import Dvcfile, is_dvc_file

from ..exceptions import (
    CacheLinkError,
    OutputDuplicationError,
    OverlappingOutputPathsError,
    RecursiveAddingWhileUsingFilename,
)
from ..output.base import OutputDoesNotExistError
from ..progress import Tqdm
from ..repo.scm_context import scm_context
from ..utils import LARGE_DIR_SIZE, resolve_paths
from . import locked

logger = logging.getLogger(__name__)


@locked
@scm_context
def add(
    repo,
    targets,
    recursive=False,
    no_commit=False,
    fname=None,
    external=False,
    glob=False,
    vdir=None,
):
    if recursive and fname:
        raise RecursiveAddingWhileUsingFilename()

    if isinstance(targets, str):
        targets = [targets]

    link_failures = []
    stages_list = []
    num_targets = len(targets)
    with Tqdm(total=num_targets, desc="Add", unit="file", leave=True) as pbar:
        if num_targets == 1:
            # clear unneeded top-level progress bar for single target
            pbar.bar_format = "Adding..."
            pbar.refresh()
        for target in targets:
            sub_targets = _find_all_targets(repo, target, recursive)
            pbar.total += len(sub_targets) - 1

            if os.path.isdir(target) and len(sub_targets) > LARGE_DIR_SIZE:
                logger.warning(
                    "You are adding a large directory '{target}' recursively,"
                    " consider tracking it as a whole instead.\n"
                    "{purple}HINT:{nc} Remove the generated DVC-file and then"
                    " run `{cyan}dvc add {target}{nc}`".format(
                        purple=colorama.Fore.MAGENTA,
                        cyan=colorama.Fore.CYAN,
                        nc=colorama.Style.RESET_ALL,
                        target=target,
                    )
                )

            stages = _create_stages(
                repo,
                sub_targets,
                fname,
                pbar=pbar,
                external=external,
                glob=glob,
                vdir=vdir,
            )

            # NOTES: This will always produce the specific file that was added:
            #   $ dvc add rpm-images/problem-E-01/B-new.png
            #
            #   stages = [Stage: 'rpm-images/problem-E-01/B-new.png.dvc']
            #
            # This happens on these both instances:
            # - full directory (usual behavior)
            # - partial directory (the new desired behavior)
            #
            # It then results in this type of error:
            #     repo.check_modified_graph(stages)
            #   *** dvc.exceptions.OverlappingOutputPathsError: Paths for outs:
            #   'rpm-images'('rpm-images.dvc')
            #   'rpm-images/problem-E-01/B-new.png'('rpm-images/problem-E-01/B-new.png.dvc')
            #   overlap. To avoid unpredictable behaviour, rerun command with non overlapping outs paths.
            #
            # But when this command is ran, no error is raised (for both full
            # and partial behaviors):
            #   $ dvc add rpm-images
            #
            #   stages = [Stage: 'rpm-images.dvc']

            # Stuff that has heppened:
            # - The 'rpm-images.dvc' file is also DELETED here only if it has
            # seen a change in the data.
            # - A .dvc/cache/e1/a0cf7fcebe1c12bc0adeaf7ca38dfd.dir file is
            # added. It also has the 'B-new.png' file declared.

            try:
                repo.check_modified_graph(stages)
            except OverlappingOutputPathsError as exc:
                msg = (
                    "Cannot add '{out}', because it is overlapping with other "
                    "DVC tracked output: '{parent}'.\n"
                    "To include '{out}' in '{parent}', run "
                    "'dvc commit {parent_stage}'"
                ).format(
                    out=exc.overlapping_out.path_info,
                    parent=exc.parent.path_info,
                    parent_stage=exc.parent.stage.addressing,
                )
                raise OverlappingOutputPathsError(
                    exc.parent, exc.overlapping_out, msg
                )
            except OutputDuplicationError as exc:
                raise OutputDuplicationError(
                    exc.output, set(exc.stages) - set(stages)
                )

            link_failures.extend(
                _process_stages(repo, stages, no_commit, pbar)
            )
            stages_list += stages

        if num_targets == 1:  # restore bar format for stats
            pbar.bar_format = pbar.BAR_FMT_DEFAULT

    if link_failures:
        msg = (
            "Some targets could not be linked from cache to workspace.\n{}\n"
            "To re-link these targets, reconfigure cache types and then run:\n"
            "\n\tdvc checkout {}"
        ).format(
            CacheLinkError.SUPPORT_LINK,
            " ".join([str(stage.relpath) for stage in link_failures]),
        )
        logger.warning(msg)

    return stages_list


def _process_stages(repo, stages, no_commit, pbar):
    link_failures = []

    with Tqdm(
        total=len(stages),
        desc="Processing",
        unit="file",
        disable=len(stages) == 1,
    ) as pbar_stages:
        for stage in stages:
            try:
                stage.save()
            except OutputDoesNotExistError:
                pbar.n -= 1
                raise

            try:
                if not no_commit:
                    stage.commit()

                    # => out.commit()
                    # => cache.save(self.path_info, self.cache.tree, self.hash_info)
                    # => cache._save(path_info, tree, hash_info, save_link, **kwargs)
                    # => cache._save_dir(path_info, tree, hash_info, save_link, **kwargs)
                    # =>


            except CacheLinkError:
                link_failures.append(stage)

            # The 'stage.commit()' is where the:
            # - the new file is added into the ./dvc/cache/xx/yy

            Dvcfile(repo, stage.path).dump(stage)

            # The 'Dvcfile.dump()' is where the:
            # - 'rpm-images.dvc' file is updated

            pbar_stages.update()

    return link_failures


def _find_all_targets(repo, target, recursive):
    if os.path.isdir(target) and recursive:
        return [
            os.fspath(path)
            for path in Tqdm(
                repo.tree.walk_files(target),
                desc="Searching " + target,
                bar_format=Tqdm.BAR_FMT_NOTOTAL,
                unit="file",
            )
            if not repo.is_dvc_internal(os.fspath(path))
            if not is_dvc_file(os.fspath(path))
            if not repo.scm.belongs_to_scm(os.fspath(path))
            if not repo.scm.is_tracked(os.fspath(path))
        ]
    return [target]


def _create_stages(
    repo, targets, fname, pbar=None, external=False, glob=False, vdir=None,
):
    from glob import iglob

    from dvc.stage import Stage, create_stage

    if glob:
        expanded_targets = [
            exp_target
            for target in targets
            for exp_target in iglob(target, recursive=True)
        ]
    else:
        expanded_targets = targets

    stages = []
    for out in Tqdm(
        expanded_targets,
        desc="Creating DVC-files",
        disable=len(expanded_targets) < LARGE_DIR_SIZE,
        unit="file",
    ):
        path, wdir, out = resolve_paths(repo, out)
        stage = create_stage(
            Stage,
            repo,
            fname or path,
            wdir=wdir,
            outs=[out],
            external=external,
            vdir=vdir,
        )
        # After 'create_stage':
        # - the *.dvc file is still intact
        # - it also has calculated a new .dvc/cache/xx/*.dir when on the orig
        #     behavior

        # >>> [x for x in stage.outs[0].cache._list_paths()]
        # ['/Users/burnzz/dev/iterative/explore/.dvc/cache/ca/33682bbdb304025242bbc1b0d79d52.dir']

        # NOTES: need to add this here before the Dvcfile is removed.
        if vdir:
            vdir.hash_info = stage.reload().outs[0].hash_info

            # inject the dependency
            stage.outs[0].tree.vdir = vdir

        if stage:
            Dvcfile(repo, stage.path).remove()
        # The *.dvc file is now deleted.

        repo._reset()  # pylint: disable=protected-access

        if not stage:
            if pbar is not None:
                pbar.total -= 1
            continue

        stages.append(stage)
        if pbar is not None:
            pbar.update_msg(out)

    return stages
