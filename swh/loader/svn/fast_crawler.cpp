// Copyright (C) 2023  The Software Heritage developers
// See the AUTHORS file at the top-level directory of this distribution
// License: GNU General Public License version 3, or any later version
// See top-level LICENSE file for more information

// This C++ extension module for Python implements a fast way to crawl a
// remote subversion repository content (aka listing all paths it contains and their
// properties) at a given revision. Unlike "svn ls --depth infinity" command it performs
// only one SVN request over the network, hence saving time especially with
// large repositories.
// Code is freely inspired from the fast-svn-crawler project from Dmitry Pavlenko
// https://sourceforge.net/projects/fastsvncrawler/
// http://vcs.atspace.co.uk/2012/07/15/subversion-remote-api-listing-repository-with-status-request/

// The crawl_repository function it contains returns a dictionary where keys
// are paths and values dictionaries holding path type (file or dir) but also
// the subversion properties associated to it.

#include <Python.h>

#include <svn_cmdline.h>
#include <svn_auth.h>
#include <svn_ra.h>

#include <string>
#include <map>
#include <vector>

struct svn_path_info {
  std::string path;
  std::string type;
  std::map<std::string, std::string> props;

  svn_path_info(const std::string &path, const std::string &type)
      : path(path), type(type) {}
};

typedef std::vector<svn_path_info> svn_paths_info;

static svn_error_t *open_root(void *edit_baton, svn_revnum_t base_revision,
                              apr_pool_t *pool, void **dir_baton) {
  svn_paths_info *repo_paths_info = (svn_paths_info *)edit_baton;
  repo_paths_info->push_back(svn_path_info("", "dir"));
  (*dir_baton) = edit_baton;
  return SVN_NO_ERROR;
}

static svn_error_t *add_directory(const char *path, void *parent_baton,
                                  const char *copyfrom_path,
                                  svn_revnum_t copyfrom_revision, apr_pool_t *pool,
                                  void **child_baton) {
  svn_paths_info *repo_paths_info = (svn_paths_info *)parent_baton;
  repo_paths_info->push_back(svn_path_info(path, "dir"));
  *child_baton = parent_baton;
  return SVN_NO_ERROR;
}

static svn_error_t *change_dir_prop(void *dir_baton, const char *name,
                                    const svn_string_t *value, apr_pool_t *pool) {
  svn_paths_info *repo_paths_info = (svn_paths_info *)dir_baton;
  if (value && value->data) {
    repo_paths_info->back().props[name] = value->data;
  }
  return SVN_NO_ERROR;
}

static svn_error_t *add_file(const char *path, void *parent_baton,
                             const char *copyfrom_path, svn_revnum_t copyfrom_revision,
                             apr_pool_t *pool, void **file_baton) {

  svn_paths_info *repo_paths_info = (svn_paths_info *)parent_baton;
  repo_paths_info->push_back(svn_path_info(path, "file"));
  *file_baton = parent_baton;

  return SVN_NO_ERROR;
}

static svn_error_t *change_file_prop(void *file_baton, const char *name,
                                     const svn_string_t *value, apr_pool_t *pool) {
  if (value && value->data) {
    svn_paths_info *repo_paths_info = (svn_paths_info *)file_baton;
    repo_paths_info->back().props[name] = value->data;
  }

  return SVN_NO_ERROR;
}

static svn_error_t *crawl(const char *url, svn_revnum_t revision, const char *username,
                          const char *password, apr_pool_t *pool,
                          svn_paths_info &repo_paths_info) {
  svn_error_t *err;

  svn_auth_baton_t *auth_baton;
  svn_ra_callbacks2_t *callbacks;
  SVN_ERR(svn_ra_create_callbacks(&callbacks, pool));

  apr_hash_t *config = NULL;
  err = svn_config_get_config(&config, NULL, pool);
  if (err) {
    if (APR_STATUS_IS_EACCES(err->apr_err) || APR_STATUS_IS_ENOTDIR(err->apr_err)) {
      svn_error_clear(err);
    } else {
      return err;
    }
  }

  svn_cmdline_init("svn-crawler", stderr);
  SVN_ERR(svn_cmdline_create_auth_baton2(
      &auth_baton, TRUE, username, password, NULL, FALSE, FALSE, FALSE, FALSE, FALSE,
      FALSE,
      static_cast<svn_config_t *>(
          apr_hash_get(config, SVN_CONFIG_CATEGORY_CONFIG, APR_HASH_KEY_STRING)),
      NULL, NULL, pool));

  callbacks->auth_baton = auth_baton;

  svn_ra_session_t *session;
  SVN_ERR(svn_ra_open4(&session, NULL, url, NULL, callbacks, NULL, config, pool));

  if (revision == SVN_INVALID_REVNUM) {
    SVN_ERR(svn_ra_get_latest_revnum(session, &revision, pool));
  }

  svn_delta_editor_t *editor = svn_delta_default_editor(pool);
  editor->open_root = open_root;
  editor->add_directory = add_directory;
  editor->add_file = add_file;
  editor->change_file_prop = change_file_prop;
  editor->change_dir_prop = change_dir_prop;

  const svn_ra_reporter3_t *status_reporter;
  void *reporter_baton;
  SVN_ERR(svn_ra_do_status2(session, &status_reporter, &reporter_baton, "", revision,
                            svn_depth_infinity, editor, &repo_paths_info, pool));

  SVN_ERR(status_reporter->set_path(reporter_baton, "", revision, svn_depth_infinity,
                                    TRUE, NULL, pool));
  SVN_ERR(status_reporter->finish_report(reporter_baton, pool));

  return SVN_NO_ERROR;
}

static PyObject *fast_crawler_crawl_repository(PyObject *, PyObject *args,
                                               PyObject *kwargs) {
  apr_pool_t *pool;
  char *repo_url;
  char *username = NULL;
  char *password = NULL;
  int revnum = SVN_INVALID_REVNUM;

  static const char *kwlist[] = {"repo_url", "revnum", "username", "password", NULL};
  if (!PyArg_ParseTupleAndKeywords(args, kwargs, "s|iss", (char **)kwlist, &repo_url,
                                   &revnum, &username, &password)) {
    return NULL;
  }

  apr_pool_initialize();
  apr_pool_create_ex(&pool, NULL, NULL, NULL);

  svn_ra_initialize(pool);

  svn_paths_info repo_paths_info;
  svn_error_t *err = crawl(repo_url, revnum, username, password, pool, repo_paths_info);

  std::string err_msg;
  if (err != NULL) {
    err_msg = err->message;
  }

  apr_pool_destroy(pool);
  apr_pool_terminate();

  if (!err_msg.empty()) {
    PyErr_SetString(PyExc_RuntimeError, err_msg.c_str());
    return NULL;
  }

  PyObject *ret = PyDict_New();

  for (auto &path_info : repo_paths_info) {
    PyObject *dict = PyDict_New();
    PyObject *props = PyDict_New();
    for (auto &it : path_info.props) {
      PyDict_SetItem(props, PyUnicode_FromString(it.first.c_str()),
                     PyUnicode_FromString(it.second.c_str()));
    }

    PyDict_SetItem(dict, PyUnicode_FromString("type"),
                   PyUnicode_FromString(path_info.type.c_str()));
    PyDict_SetItem(dict, PyUnicode_FromString("props"), props);
    PyDict_SetItem(ret, PyUnicode_FromString(path_info.path.c_str()), dict);
  }

  return ret;
}

static PyMethodDef fast_crawler_methods[] = {
    {"crawl_repository", (PyCFunction)fast_crawler_crawl_repository,
     METH_VARARGS | METH_KEYWORDS,
     "crawl_repository(repo_url, revnum = -1, username = '', password = '')\n--\n\n"
     "List remote subversion repository content at a given revision in a fast way.\n\n"
     "Args:\n"
     "    repo_url (str): URL of subversion repository to crawl\n"
     "    revnum (int): revision number to crawl repository at, use ``HEAD`` by default\n"
     "        if not provided\n"
     "    username (str): optional username if repository access requires credentials\n"
     "    password (str): optional password if repository access requires credentials\n"
     "Returns:\n"
     "    Dict[str, Dict[str, Any]]: A dictionary whose keys are repository paths and \n"
     "    values dictionaries holding path type (``file`` or ``dir``) but also the \n"
     "    subversion properties associated to it.\n\n"
     "Raises:\n"
     "    RuntimeError: if an error occurs when calling subversion C API\n\n"},
    {NULL, NULL, 0, NULL} /* Sentinel */
};

static struct PyModuleDef fast_crawler_module_def = {
    PyModuleDef_HEAD_INIT,
    "fast_crawler", /* m_name */
    "C++ extension module for Python implementing a fast way to crawl a "
    "remote subversion repository content (aka listing all paths it contains and their "
    "properties) at a given revision. Unlike ``svn ls --depth infinity`` command it "
    "performs only one SVN request over the network, hence saving time especially with "
    "large repositories.\n"
    "Code is freely inspired from the ``fast-svn-crawler`` project from Dmitry Pavlenko, "
    "see https://sourceforge.net/projects/fastsvncrawler/ and "
    "http://vcs.atspace.co.uk/2012/07/15/"
    "subversion-remote-api-listing-repository-with-status-request/ "
    "for more details",   /* m_doc */
    -1,                   /* m_size */
    fast_crawler_methods, /* m_methods */
    NULL,                 /* m_reload */
    NULL,                 /* m_traverse */
    NULL,                 /* m_clear */
    NULL,                 /* m_free */
};

PyMODINIT_FUNC PyInit_fast_crawler(void) {
  return PyModule_Create(&fast_crawler_module_def);
}