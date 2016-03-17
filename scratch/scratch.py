# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pysvn
from pysvn import opt_revision_kind, Revision

client = pysvn.Client()

# http://pysvn.tigris.org/docs/pysvn_prog_ref.html#pysvn_client_commit_info_style
# When set to 1 pysvn returns a dictionary of commit information
# including date, author, revision and post_commit_err.
client.commit_info_style = 1


pysvn.wc_notify_action.update_completed

# from doc
# http://pysvn.tigris.org/docs/pysvn_prog_ref.html#pysvn_client_checkout
# Note: Subversion seems to return 0 rather then the actual
# revision. Use a notify callback and record the revision reported for
# the pysvn.wc_notify_action.update_completed event. This is what the
# svn command does.
# so: http://pysvn.tigris.org/docs/pysvn_prog_ref.html#pysvn_client_callback_notify
checkout_data = {}

def checkout_notify_callback_hack(event_dict,
                                  checkout_data=checkout_data):
    if event_dict['action'] == pysvn.wc_notify_action.update_completed:
        checkout_data['url'] = event_dict['path']
        checkout_data['revision'] = event_dict['revision']

client.callback_notify = checkout_notify_callback_hack

# svn repo samples
# https://github.com/schacon/simplegit
# https://github.com/schacon/kidgloves

# In [138]: client.checkout('https://github.com/schacon/simplegit',
#                 path='/home/tony/work/inria/repo/swh-environment/swh-loader-svn/repo/simplegit')

#    .....:
# Out[138]: <Revision kind=number 0>

# In [155]: client.update('.')
# Out[155]: [<Revision kind=number 4>]


# In [109]: svnrepo
# Out[109]: '/home/tony/work/inria/repo/swh-environment/swh-loader-svn/repo/kidgloves'


# In [106]: client.update('.')
# Out[106]: [<Revision kind=number 19>]


# In [104]: client.status('.')
# Out[104]:
# [<PysvnStatus '.'>,
#  <PysvnStatus 'README'>,
#  <PysvnStatus 'example.rb'>,
#  <PysvnStatus 'kidgloves.rb'>]


# In [105]: client.ls('.')
# Out[105]:
# [<PysvnDirent 'kidgloves.rb'>,
#  <PysvnDirent 'README'>,
#  <PysvnDirent 'example.rb'>]

# instanciate a revision number
rev1 = pysvn.Revision(opt_revision_kind.number, 1)

# In [107]: client.cat('example.rb', revision=pysvn.Revision( pysvn.opt_revision_kind.number, 19 ))
# Out[107]: b'require \'rubygems\'\nrequire \'rack\'\nrequire \'kidgloves\'\n\nclass HelloWorld\n  def call(env)\n    [200, {"Content-Type" => "text/html"}, ["Hello world!"]]\n  end\nend\n\nRack::Handler::KidGloves.run HelloWorld.new\n'

# In [108]: client.cat('example.rb', revision=pysvn.Revision( pysvn.opt_revision_kind.number, 18 ))
# Out[108]: b'require \'rubygems\'\nrequire \'rack\'\nrequire \'kidgloves\'\n\nclass HelloWorld\n  def call(env)\n    [200, {"Content-Type" => "text/html"}, ["Hello world!"]]\n  end\nend\n\nRack::Handler::KidGloves.run HelloWorld.new\n'

for f, info in client.info2('.',
                            revision=Revision(pysvn.opt_revision_kind.number, 1),
                            peg_revision=Revision(opt_revision_kind.unspecified)):
    print('#### %s:' % f)
    for k, v in info.items():
        print('%s: %s' % (k, v))


s = client.status('.')
for i in s:
    print('#### %s:' % i)
    for k, v in i.items():
        print('%s: %s' % (k, v))


for log in client.log('.',
                      revision_start=Revision( opt_revision_kind.head ),
                      revision_end=Revision( opt_revision_kind.number, 0)):


    for k, v  in log.items():
        print('%s: %s' % (k, v))
# date: 1240030591.0
# changed_paths: []
# message: changed the verison number

# revprops: {'git-commit': 'ca82a6dff817ec66f44342007202690a93763949', 'svn:date': 1240030591.0, 'svn:author': 'scott.chacon', 'svn:log': 'changed the verison number\n', 'git-ref': 'refs/heads/master'}
# revision: <Revision kind=number 3>
# author: scott.chacon
# has_children: 0
# date: 1240030553.0
# changed_paths: []
# message: removed unnecessary test code

# revprops: {'git-commit': '085bb3bcb608e1e8451d4b2432f8ecbe6306e7e7', 'svn:date': 1240030553.0, 'svn:author': 'scott.chacon', 'svn:log': 'removed unnecessary test code\n', 'git-ref': 'refs/heads/master'}
# revision: <Revision kind=number 2>
# author: scott.chacon
# has_children: 0
# date: 1205602288.0
# changed_paths: []
# message: first commit

# revprops: {'git-commit': 'a11bef06a3f659402fe7563abf99ad00de2209e6', 'svn:date': 1205602288.0, 'svn:author': 'scott.chacon', 'svn:log': 'first commit\n', 'git-ref': 'refs/heads/master'}
# revision: <Revision kind=number 1>
# author: scott.chacon
# has_children: 0


# pysvn.depth.infinity



import svn.local
import svn.remote

l = svn.local.LocalClient('/home/tony/work/inria/repo/swh-environment/swh-loader-svn/repo/simplegit')
linfo = l.info()
print(linfo)

entries = l.list()
for entry in entries:
    print(entry)

for rel_path, elem in l.list_recursive():
    print('\n[%s]\n%s' % (rel_path, elem))

# r = svn.remote.RemoteClient('http://svn.apache.org/repos/asf/tomcat')
# rinfo = r.info()
# print(rinfo)
