# SPDX-License-Identifier: Apache-2.0
# Copyright Contributors to the Rez Project


from rez.packages import get_latest_package
from rez.version import Version
from rez.vendor.distlib.database import DistributionPath
from rez.resolved_context import ResolvedContext
from rez.utils.execution import Popen
from rez.utils.pip import get_rez_requirements, pip_to_rez_package_name, \
    pip_to_rez_version
from rez.utils.logging_ import print_debug, print_info, print_error, \
    print_warning
from rez.exceptions import BuildError, PackageFamilyNotFoundError, \
    PackageNotFoundError
from rez.package_maker import make_package
from rez.config import config

import os
from shlex import quote
from pprint import pformat
from enum import Enum
import re
import shutil
from tempfile import mkdtemp
from textwrap import dedent
from io import StringIO


class InstallMode(Enum):
    # don't install dependencies. Build may fail, for example the package may
    # need to compile against a dependency. Will work for pure python though.
    no_deps = 0
    # only install dependencies that we have to. If an existing rez package
    # satisfies a dependency already, it will be used instead. The default.
    min_deps = 1


def find_uv():
    """Find uv."""
    if uv_exe := shutil.which("uv"):
        return uv_exe

    raise RuntimeError("Cant find uv.exe in PATH")


def find_python_from_context(python_version):
    """Find python from rez context.

    Args:
        python_version (str or `Version`): Python version to use

    Returns:
        3-tuple:
        - str: Python executable or None if we fell back to system pip.
        - str: Pip version or None if we fell back to system pip.
        - `ResolvedContext`: Context containing pip, or None if we fell back
          to system pip.
    """
    target = "python"

    if python_version:
        ver = Version(str(python_version))
        python_major_minor_ver = ver.trim(2)
    else:
        # use latest major.minor
        package = get_latest_package("python")
        if package:
            python_major_minor_ver = package.version.trim(2)
        else:
            raise BuildError("Found no python rez package.")

    python_package = "python-%s" % str(python_major_minor_ver)

    try:
        context = ResolvedContext([python_package])
    except (PackageFamilyNotFoundError, PackageNotFoundError):
        print_debug("No rez package called %s found", target)
        return None

    return context


def uv_install_package(source_name, python_version=None,
                       mode=InstallMode.min_deps, release=False, prefix=None,
                       extra_args=None):
    """Install a uv-compatible python package as a rez package.
    Args:
        source_name (str): Name of package or archive/url containing the uv
            package source. This is the same as the arg you would pass to
            the 'uv pip install' command.
        python_version (str or `Version`): Python version to use to perform the
            install, and subsequently have the resulting rez package depend on.
        mode (`InstallMode`): Installation mode, determines how dependencies are
            managed.
        release (bool): If True, install as a released package; otherwise, it
            will be installed as a local package.
        extra_args (List[str]): Additional options to the uv pip install command.

    Returns:
        2-tuple:
            List of `Variant`: Installed variants;
            List of `Variant`: Skipped variants (already installed).
    """
    installed_variants = []
    skipped_variants = []

    uv_exe = find_uv()
    context = find_python_from_context(python_version)
    # determine version of python in use
    python_variant = context.get_resolved_package("python")
    py_ver = python_variant.version

    print_info(
        "Installing %r with uv taken from %r",
        source_name, uv_exe
    )

    # TODO: should check if packages_path is writable before continuing with pip
    #
    if prefix is not None:
        packages_path = prefix
    else:
        packages_path = (config.release_packages_path if release
                         else config.local_packages_path)

    targetpath = mkdtemp(suffix="-rez", prefix=f"uv-{py_ver}-")

    if context and config.debug("package_release"):
        buf = StringIO()
        print("\n\npackage download environment:", file=buf)
        context.print_info(buf)
        _log(buf.getvalue())

    _extra_args = extra_args or config.pip_extra_args or []
    try:
        _extra_args.remove("--no-deps")
    except ValueError:
        no_deps = False
    else:
        no_deps = True

    install_cmd = [uv_exe, "pip", "install",
                   "--prerelease=explicit",
                   "--index-strategy=unsafe-best-match",
                   "--system",
                   f"--python-version={python_version}",
                   "--python-preference=system",
                   "--no-python-downloads",
                   "--compile"]

    if os.path.isdir(source_name):
        # Build uv pip commandline
        compile_cmd = [uv_exe, "pip", "compile" , "pyproject.toml",
                       "-o", f"{targetpath}/uv.lock", "--upgrade",
                       "--prerelease=explicit",
                       "--emit-index-url",
                       "--index-strategy=unsafe-best-match",
                       "--system",
                       f"--python-version={python_version}",
                        "--python-preference=system",
                        "--no-python-downloads"]

        sync_cmd = [uv_exe, "pip", "sync",
                    "--index-strategy=unsafe-best-match",
                    "--system",
                    f"--python-version={python_version}",
                    "--python-preference=system",
                    "--no-python-downloads",
                    "--compile", f"--target={targetpath}"]

        if mode == InstallMode.no_deps and "--no-deps" not in _extra_args:
            compile_cmd.append("--no-deps")

        compile_cmd.extend(_extra_args)
        sync_cmd.extend(_extra_args)

        print(" ".join( compile_cmd))
        _cmd(context=None, command=compile_cmd)

        sync_cmd.append(f"{targetpath}/uv.lock")
        if not no_deps:
            print(" ".join(sync_cmd))
            _cmd(context=None, command=sync_cmd)

        install_cmd.append(f"--override={targetpath}/uv.lock")

    if mode == InstallMode.no_deps and "--no-deps" not in _extra_args:
        install_cmd.append("--no-deps")

    if not _option_present(_extra_args, "-t", "--target"):
        install_cmd.append(f"--target={targetpath}")

    install_cmd.extend(_extra_args)
    install_cmd.append(source_name)
    # run uv
    #
    # Note: https://github.com/pypa/pip/pull/3934. If/when this PR is merged,
    # it will allow explicit control of where to put bin files.
    #
    print(" ".join(install_cmd))
    _cmd(context=None, command=install_cmd)

    # Collect resulting python packages using distlib
    distribution_path = DistributionPath([targetpath])
    distributions = list(distribution_path.get_distributions())
    dist_names = [x.name for x in distributions]

    def log_append_pkg_variants(pkg_maker):
        template = '{action} [{package.qualified_name}] {package.uri}{suffix}'
        actions_variants = [
            (
                print_info, 'Installed',
                installed_variants, pkg_maker.installed_variants or [],
            ),
            (
                print_debug, 'Skipped',
                skipped_variants, pkg_maker.skipped_variants or [],
            ),
        ]
        for print_, action, variants, pkg_variants in actions_variants:
            for variant in pkg_variants:
                variants.append(variant)
                package = variant.parent
                suffix = (' (%s)' % variant.subpath) if variant.subpath else ''
                print_(template.format(**locals()))

    # get list of package and dependencies
    for distribution in distributions:
        # convert uv pip requirements into rez requirements
        rez_requires = get_rez_requirements(
            installed_dist=distribution,
            python_version=py_ver,
            name_casings=dist_names
        )

        # log the pip -> rez requirements translation, for debugging
        _log(
            "Uv Pip to rez requirements translation information for "
            + distribution.name_and_version
            + ":\n"
            + pformat({
                "pip": {
                    "run_requires": map(str, distribution.run_requires)
                },
                "rez": rez_requires
            })
        )

        # determine where yv pip files need to be copied into rez package
        src_dst_lut = _get_distribution_files_mapping(distribution, targetpath)
        # build tools list
        tools = []
        for relpath in src_dst_lut.values():
            dir_, filename = os.path.split(relpath)
            if dir_ == "bin":
                tools.append(filename)

        # Sanity warning to see if any files will be copied
        if not src_dst_lut:
            message = 'No source files exist for {}!'
            if not _verbose:
                message += '\nTry again with rez-pip --verbose ...'
            print_warning(message.format(distribution.name_and_version))

        def make_root(variant, path):
            """Using distlib to iterate over all installed files of the current
            distribution to copy files to the target directory of the rez package
            variant
            """
            for rel_src, rel_dest in src_dst_lut.items():
                src = os.path.join(targetpath, rel_src)
                dest = os.path.join(path, rel_dest)

                if not os.path.exists(os.path.dirname(dest)):
                    os.makedirs(os.path.dirname(dest))

                shutil.copyfile(src, dest)

                if _is_exe(src):
                    shutil.copystat(src, dest)

        # create the rez package
        name = pip_to_rez_package_name(distribution.name)
        version = pip_to_rez_version(distribution.version)
        requires = rez_requires["requires"]
        variant_requires = rez_requires["variant_requires"]
        metadata = rez_requires["metadata"]

        with make_package(name, packages_path, make_root=make_root) as pkg:
            # basics (version etc)
            pkg.version = version

            if distribution.metadata.summary:
                pkg.description = distribution.metadata.summary

            # requirements and variants
            if requires:
                pkg.requires = requires

            if variant_requires:
                pkg.variants = [variant_requires]

            # commands
            commands = []
            commands.append("env.PYTHONPATH.append('{root}/python')")

            if tools:
                pkg.tools = tools
                commands.append("env.PATH.append('{root}/bin')")

            pkg.commands = '\n'.join(commands)

            # Make the package use hashed variants. This is required because we
            # can't control what ends up in its variants, and that can easily
            # include problematic chars (>, +, ! etc).
            # TODO: #672
            #
            pkg.hashed_variants = True

            # add some custom attributes to retain pip-related info
            pkg.pip_name = distribution.name_and_version
            pkg.from_pip = True
            pkg.is_pure_python = metadata["is_pure_python"]

            distribution_metadata = distribution.metadata.todict()

            help_ = []

            if "home_page" in distribution_metadata:
                help_.append(["Home Page", distribution_metadata["home_page"]])

            if "download_url" in distribution_metadata:
                help_.append(["Source Code", distribution_metadata["download_url"]])

            if help_:
                pkg.help = help_

            if "author" in distribution_metadata:
                author = distribution_metadata["author"]

                if "author_email" in distribution_metadata:
                    author += ' ' + distribution_metadata["author_email"]

                pkg.authors = [author]

        log_append_pkg_variants(pkg)

    # cleanup
    shutil.rmtree(targetpath)

    # print summary
    #
    if installed_variants:
        print_info("%d packages were installed.", len(installed_variants))
    else:
        print_warning("NO packages were installed.")
    if skipped_variants:
        print_warning(
            "%d packages were already installed.",
            len(skipped_variants),
        )

    return installed_variants, skipped_variants


def _is_exe(fpath):
    return os.path.exists(fpath) and os.access(fpath, os.X_OK)


def _get_distribution_files_mapping(distribution, targetdir):
    """Get remapping of uv pip installation to rez package installation.

    Args:
        distribution (`distlib.database.InstalledDistribution`): The installed
            distribution
        targetdir (str): Where distribution was installed to (via uv pip --target)

    Returns:
        Dict of (str, str):
        * key: Path of uv pip installed file, relative to `targetdir`;
        * value: Relative path to install into rez package.
    """
    def get_mapping(rel_src):
        topdir = rel_src.split(os.sep)[0]

        if topdir == "bin":
            return (rel_src, rel_src)

        # Special case - dist-info files. These are all in a '<pkgname>-<version>.dist-info'
        # dir. We keep this dir and place it in the root 'python' dir of the rez package.
        #
        if topdir.endswith(".dist-info"):
            rel_dest = os.path.join("python", rel_src)
            return (rel_src, rel_dest)

        # Remapping of other installed files according to manifest
        if topdir == os.pardir:
            for remap in config.pip_install_remaps:
                path = remap['record_path']
                if re.search(path, rel_src):
                    pip_subpath = re.sub(path, remap['pip_install'], rel_src)
                    rez_subpath = re.sub(path, remap['rez_install'], rel_src)
                    return (pip_subpath, rez_subpath)

            tokenised_path = rel_src.replace(os.pardir, '{pardir}')
            tokenised_path = tokenised_path.replace(os.sep, '{sep}')
            dist_record = '{dist.name}-{dist.version}.dist-info{os.sep}RECORD'
            dist_record = dist_record.format(dist=distribution, os=os)

            try_this_message = r"""
            Unknown source file in {0}! '{1}'

            To resolve, try:

            1. Manually install the uv pip package using 'uv pip install --target'
               to a temporary location.
            2. See where '{1}'
               actually got installed to by uv pip, RELATIVE to --target location
            3. Create a new rule to 'pip_install_remaps' configuration like:

                {{
                    "record_path": r"{2}",
                    "pip_install": r"<RELATIVE path uv pip installed to in 2.>",
                    "rez_install": r"<DESTINATION sub-path in rez package>",
                }}

            4. Try rez-pip install again.

            If path remapping is not enough, consider submitting a new issue
            via https://github.com/AcademySoftwareFoundation/rez/issues/new
            """.format(dist_record, rel_src, tokenised_path)
            print_error(dedent(try_this_message).lstrip())

            raise IOError(
                89,  # errno.EDESTADDRREQ : Destination address required
                "Don't know what to do with relative path in {0}, see "
                "above error message for".format(dist_record),
                rel_src,
            )

        # At this point the file should be <pkg-name>/..., so we put
        # into 'python' subdir in rez package.
        #
        rel_dest = os.path.join("python", rel_src)
        return (rel_src, rel_dest)

    # iterate over uv installed files
    result = {}
    for installed_file in distribution.list_installed_files():
        rel_src_orig = os.path.normpath(installed_file[0])
        rel_src, rel_dest = get_mapping(rel_src_orig)

        src_filepath = os.path.join(targetdir, rel_src)
        if not os.path.exists(src_filepath):
            print_warning(
                "Skipping non-existent source file: %s (%s)",
                src_filepath, rel_src_orig
            )
            continue

        result[rel_src] = rel_dest

    return result


def _option_present(opts, *args):
    for opt in opts:
        for arg in args:
            if opt == arg or opt.startswith(arg + '='):
                return True
    return False


def _cmd(context, command):
    cmd_str = ' '.join(quote(x) for x in command)
    _log("running: %s" % cmd_str)

    if context is None:
        p = Popen(command)
    else:
        p = context.execute_shell(command=command, block=False)

    with p:
        p.wait()

    if p.returncode:
        raise BuildError("Failed to download source with uv: %s" % cmd_str)


_verbose = config.debug("package_release")


def _log(msg):
    if _verbose:
        print_debug(msg)
