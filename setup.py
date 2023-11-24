#!/usr/bin/env python3
# Copyright (C) 2015-2023  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import errno
import os
import shlex
import subprocess

from setuptools import Extension, setup

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
    ext_modules=[
        SvnExtension(
            "swh.loader.svn.fast_crawler",
            ["swh/loader/svn/fast_crawler.cpp"],
            libraries=["svn_delta-1", "svn_ra-1", "svn_subr-1"],
        )
    ],
)
