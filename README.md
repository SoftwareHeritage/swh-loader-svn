swh-loader-svn
==============

The Software Heritage SVN Loader is a tool and a library to walk a remote svn repository
and inject into the SWH dataset all contained files that weren't known before.

The main entry points are

- :class:`swh.loader.svn.loader.SvnLoader` for the main svn loader which ingests content out of
  a remote svn repository

- :class:`swh.loader.svn.loader.SvnLoaderFromDumpArchive` which mounts a repository out of a
  svn dump prior to ingest it.

- :class:`swh.loader.svn.loader.SvnLoaderFromRemoteDump` which mounts a repository with
  svnrdump prior to ingest its content.

# CLI run

With the configuration:

/tmp/loader_svn.yml:
```
storage:
  cls: remote
  args:
    url: http://localhost:5002/
```

Run:

```
swh loader --config-file /tmp/loader_svn.yml \
    run svn <svn-repository-url>
```
