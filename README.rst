Software Heritage - Subversion loader |build status|
====================================================

.. |build status| image:: https://jenkins.softwareheritage.org/job/DLDSVN/job/master/badge/icon
   :target: https://jenkins.softwareheritage.org/job/DLDSVN/job/master/

The Software Heritage Subversion loader is a tool and a library to walk a
remote svn repository and inject into the Software Heritage archive all
contained files, directories and commits that weren't known before.

The main entry points are

- ``swh.loader.svn.loader.SvnLoader`` for the main svn loader which ingests
  content out of a remote svn repository

- ``swh.loader.svn.loader.SvnLoaderFromDump`` which mounts a repository
  out of a svn dump prior to ingest it.

- ``swh.loader.svn.loader.SvnLoaderFromRemoteDump`` which mounts a repository
  out of a svn dump obtained with svnrdump prior to ingest its content.

- ``swh.loader.svn.directory.SvnExportLoader`` which ingests an svn tree at a
  specific revision.

CLI run
-------

With the configuration:

``/tmp/loader_svn.yml``:

.. code-block:: yaml

   storage:
     cls: remote
     args:
       url: http://localhost:5002/

Run:

.. code-block:: shell

   $ swh loader --config-file /tmp/loader_svn.yml run svn <svn-repository-url>
