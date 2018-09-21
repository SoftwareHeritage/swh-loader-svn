swh-loader-svn
==============

Documents are in the ./docs folder:
- Specification: ./docs/swh-loader-svn.txt
- Comparison performance with git-svn: ./docs/comparison-git-svn-swh-svn.org

# Configuration file

## Location

Either:
- /etc/softwareheritage/
- ~/.config/swh/
- ~/.swh/

Note: Will call that location $SWH_CONFIG_PATH

## Configuration sample

$SWH_CONFIG_PATH/loader/svn.yml:
```
storage:
  cls: remote
  args:
    url: http://localhost:5002/

check_revision: 10
```

## configuration content

With at least the following module (swh.loader.svn.tasks) and queue
(swh_loader_svn):

$SWH_CONFIG_PATH/worker.yml:
```
task_broker: amqp://guest@localhost//
task_modules:
task_modules:
  - swh.loader.svn.tasks
task_queues:
  - swh_loader_svn
task_soft_time_limit = 0
```

`swh.loader.svn.tasks` and `swh_loader_svn` are the important entries here.

## toplevel

### local svn repository

```
$ python3
repo = 'pyang-repo-r343-eol-native-mixed-lf-crlf'
#repo = 'zipeg-gae'
origin_url = 'http://%s.googlecode.com' % repo
local_repo_path = '/home/storage/svn/repo'
svn_url = 'file://%s/%s' % (local_repo_path, repo)

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.svn.tasks import LoadSvnRepository

t = LoadSvnRepository()
t.run(svn_url=svn_url,
      origin_url=origin_url, visit_date='2016-05-03T15:16:32+00:00',
      start_from_scratch=True)
```

### repository dump

```
$ python3
repo = '0-512-md'
archive_name = '%s-repo.svndump.gz' % repo
archive_path = '/home/storage/svn/dumps/%s' % archive_name
origin_url = 'http://%s.googlecode.com' % repo
svn_url = 'file://%s' % repo

import logging
logging.basicConfig(level=logging.DEBUG)

from swh.loader.svn.tasks import MountAndLoadSvnRepository

t = MountAndLoadSvnRepository()
t.run(archive_path=archive_path,
      origin_url=origin_url,
      visit_date='2016-05-03T15:16:32+00:00',
      start_from_scratch=True)
```

## Production like

start worker instance

To start a current worker instance:

```sh
python3 -m celery worker --app=swh.scheduler.celery_backend.config.app \
                --pool=prefork \
                --concurrency=10 \
                -Ofair \
                --loglevel=debug 2>&1
```

## Produce a repository to load

You can see:

`python3 -m swh.loader.svn.producer svn --help`

### one repository
```sh
python3 -u -m swh.loader.svn.producer svn --svn-url file:///home/storage/svn/repos/pkg-fox --visit-date 'Tue, 3 May 2017 17:16:32 +0200'
```

Note:
- `--visit-date` to override the default visit-date to now.

### multiple repositories

```sh
cat ~/svn-repository-list | python3 -m swh.loader.svn.producer svn
```

The file svn-repository-list contains a list of svn repository urls
(one per line), something like:

```txt
svn://svn.debian.org/svn/pkg-fox/ optional-url
svn://svn.debian.org/svn/glibc-bsd/ optional-url
svn://svn.debian.org/svn/pkg-voip/ optional-url
svn://svn.debian.org/svn/python-modules/ optional-url
svn://svn.debian.org/svn/pkg-gnome/ optional-url
```

## Produce archive of svndumps list to load

see. `python3 -m swh.loader.svn.producer svn-archive --help`
