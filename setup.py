#!/usr/bin/env python3
# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import errno
from io import open
import os
import shlex
import subprocess

from setuptools import find_packages, setup
from setuptools.extension import Extension

here = os.path.abspath(os.path.dirname(__file__))

# Get the long description from the README file
with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()


def parse_requirements(name=None):
    if name:
        reqf = "requirements-%s.txt" % name
    else:
        reqf = "requirements.txt"

    requirements = []
    if not os.path.exists(reqf):
        return requirements

    with open(reqf) as f:
        for line in f.readlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            requirements.append(line)
    return requirements


##############################################################################################
# The code below is taken from the setup.py file of the subvertpy project by Jelmer Vernooij
# https://github.com/jelmer/subvertpy/blob/9a3d963e6cea8480e3efa06d78c50980769ce486/setup.py
# License: GNU Lesser General Public License v2.1


def config_value(command, env, args):
    command = os.environ.get(env, command)
    try:
        return subprocess.check_output([command] + args).strip().decode()
    except OSError as e:
        if e.errno == errno.ENOENT:
            raise Exception(
                "%s not found. Please set %s environment variable" % (command, env)
            )
        raise


def split_shell_results(line):
    return shlex.split(line)


def apr_config(args):
    return config_value("apr-1-config", "APR_CONFIG", args)


def apr_build_data():
    """Determine the APR header file location."""
    try:
        includedir = os.environ["APR_INCLUDE_DIR"]
    except KeyError:
        includedir = apr_config(["--includedir"])
    if not os.path.isdir(includedir):
        raise Exception("APR development headers not found")
    try:
        extra_link_flags = split_shell_results(os.environ["APR_LINK_FLAGS"])
    except KeyError:
        extra_link_flags = split_shell_results(apr_config(["--link-ld", "--libs"]))
    return (includedir, extra_link_flags)


def svn_build_data():
    """Determine the Subversion header file location."""
    if "SVN_HEADER_PATH" in os.environ and "SVN_LIBRARY_PATH" in os.environ:
        return ([os.getenv("SVN_HEADER_PATH")], [os.getenv("SVN_LIBRARY_PATH")], [])
    svn_prefix = os.getenv("SVN_PREFIX")
    if svn_prefix is None:
        basedirs = ["/usr/local", "/usr"]
        for basedir in basedirs:
            includedir = os.path.join(basedir, "include/subversion-1")
            if os.path.isdir(includedir):
                svn_prefix = basedir
                break
    if svn_prefix is not None:
        return (
            [os.path.join(svn_prefix, "include/subversion-1")],
            [os.path.join(svn_prefix, "lib")],
            [],
        )
    raise Exception(
        "Subversion development files not found. "
        "Please set SVN_PREFIX or (SVN_LIBRARY_PATH and "
        "SVN_HEADER_PATH) environment variable. "
    )


(apr_includedir, apr_link_flags) = apr_build_data()
(svn_includedirs, svn_libdirs, svn_link_flags) = svn_build_data()


class SvnExtension(Extension):
    def __init__(self, name, *args, **kwargs):
        kwargs["include_dirs"] = [apr_includedir] + svn_includedirs
        kwargs["library_dirs"] = svn_libdirs
        kwargs["extra_link_args"] = apr_link_flags + svn_link_flags
        Extension.__init__(self, name, *args, **kwargs)


# end of code taken from subvertpy project
##############################################################################################

setup(
    name="swh.loader.svn",
    description="Software Heritage Loader SVN",
    long_description=long_description,
    long_description_content_type="text/markdown",
    python_requires=">=3.7",
    author="Software Heritage developers",
    author_email="swh-devel@inria.fr",
    url="https://forge.softwareheritage.org/diffusion/DLDSVN",
    packages=find_packages(),
    scripts=[],
    install_requires=parse_requirements() + parse_requirements("swh"),
    setup_requires=["setuptools-scm"],
    use_scm_version=True,
    extras_require={"testing": parse_requirements("test")},
    include_package_data=True,
    entry_points="""
        [swh.workers]
        loader.svn=swh.loader.svn:register
        loader.svn-export=swh.loader.svn:register_export
    """,
    classifiers=[
        "Programming Language :: Python :: 3",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
    ],
    project_urls={
        "Bug Reports": "https://forge.softwareheritage.org/maniphest",
        "Funding": "https://www.softwareheritage.org/donate",
        "Source": "https://forge.softwareheritage.org/source/swh-loader-svn",
        "Documentation": "https://docs.softwareheritage.org/devel/swh-loader-svn/",
    },
    ext_modules=[
        SvnExtension(
            "swh.loader.svn.fast_crawler",
            ["swh/loader/svn/fast_crawler.cpp"],
            libraries=["svn_delta-1", "svn_ra-1", "svn_subr-1"],
        )
    ],
)
