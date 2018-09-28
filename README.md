swh-loader-svn
==============

Documents are in the ./docs folder:
- Specification: ./docs/swh-loader-svn.txt

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
```

## Local run

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

### Mount and load an archive repository dump

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
