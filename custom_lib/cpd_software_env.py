"""
cpd_software_env can automatically install Python code and data.

A custom software environment is more than just a collection of Python code libraries.
It consists of
* a base environment that is provided by the platform such as CP4D
* a custom selection of Python packages to be loaded from a repository
* custom Python code, e.g., in .py files
* additional "data" files such as control tables and language dictionaries


cpd_software_env can wrap the content of the custom software environment
into a single package that can easily be transferred into a deployment space.
A simple statement `import cpd_software_env` in the deployed application code
will install the content. The content can then be used in the application code
in the same way as in the development environment.


The content of a software environment can be defined using a yaml notation.
The main keys are `files` , `pip` (conda tdb), and `assets`.
We illustrate the structure by way of example:

```
name: MyEnv
  # The name will be used as the name of the new Watson Studio Environment asset.
  # Default is the name of your file without the .yaml extension.

base: default_py3.7_opence   # = default  
  # Define the WStudio software specification to use as a base.
  # Custom software environments are always built on top of an existing default
  # environment in Watson Studio. Note that the value of 'base:' 
  # is the name of a software specification, it's not the name of the Environment.


files:
  - somefile        # add to the swenv package
  - somecode.py     # add to the swenv package and 
                    # insert the full path to the .py file to sys.path
  - somedir/        # add somedir and all its files to the swenv package, 
                    # insert the full path of the directory to sys.path
  # file paths are interpreted relative to the location of 
  # the software environment definition file. This default can be overridden by
  - path: myfile.py
    root: /some/project/dir

pip:
    - reqA
    - reqB==1.5
    - ...
    # req can be anything that is supported as one line in a pip requirements file
    # https://pip.pypa.io/en/stable/cli/pip_install/#requirements-file-format

assets:
    # referring to assets in the current project or deployment space
    - name: "My project asset" 
      type: data_asset
    - name: mymodule
      type: script
    - myutil.py
    # File names with .py suffix can be used unter assets: as a shorthand for
    #     name: filename
    #     type: script
    # This mimicks how Watson Studio maps JupyerLab files to project assets during git pull
    ...
```

cpd_software_env adds paths to Python sys.path in both the development and in the deployment environment. The intent is to make sure that the Python modules contained in the user's files and assets can be imported using a plain `import` statement.

Default locations of the cpd_software_env files:

Define the content in a file `cpd_software_env.yaml` 
located in the same directory as cpd_software_env.py

    .../cpd_software_env.py      # this script
    .../cpd_software_env.yaml    # defines content in yaml format

Todo: use cpd_software_env.yaml in the current directory.

The automatic installer is invoked by a plain import statement in your application

    import cpd_software_env
    
This assumes that the file cpd_software_env.py is in your Python search path.
The list of files and directories that make up the custom software environment
can be defined in the file cpd_software_env.yaml  If this file
does not exists, the directory "cpd_software_env/" next to the Python file
is used as the default.


cpd_software_env can wrap all files into a custom "software specification" 
to be used in a WML deployment space.
A simple "import cpd_software_env" in your deployed code will trigger the
same installation procedure as above.
This works for both online and batch deployments.

"""

# To see or debug what's going on whule using this module, add the following lins to your code
# import os,logging
# os.environ["CPD_SOFTWARE_ENV_VERBOSE"] = "Y"
# logging.basicConfig(level=logging.DEBUG)


import os  # getenv
import logging
from pathlib import Path

# print("__name__ =",__name__) # name of this module
# print("__file__ =",__file__) # path of this file


PkgInitialized = False

SoftwareEnv = None
# Keeps track of the state of the software env
# Is also used in wrap_into_swenv() and _create_autoinstall_sdist()

AssetsDownloadPending = None
# list of assets that still need to be downloaded
# delayed until scoring call passes the space id and user token

CurrentSwspecName = None
CurrentSwspecId = None
# Cached result of _build_and_cache_swspec()
# Reset to None when add_swenv..() is called


def get_path():
    """Get path of package directory, i.e., where this .py file is stored.
    In a deployment this dir is also the place where the copied project files are stored."""
    from pathlib import Path

    dir = Path(__file__).parent
    return str(dir)



PkgMsgs = []

def _my_print_msg(msg):
    PkgMsgs.append(msg)
    if os.getenv("CPD_SOFTWARE_ENV_VERBOSE"):
        import sys

        sys.stderr.flush()  # flush logging messages
        print("..swenv:", msg)


def get_msgs():
    """Return current list of messages that have been printed
    when the environment variable $CPD_SOFTWARE_ENV_VERBOSE is set.
    """
    return PkgMsgs



def default_swenv():
    """The user's default sw env is read when this module is used in an import statement.
    The sw env can be defined in the directory where this .py module is stored.
    Either in a file cpd_software_env.yaml
    or as a subdirectory cpd_software_env/

    Return a tuple  (dictionary, Path).
    """
    # from pathlib import Path

    swenv = {}
    my_dir = Path(__file__).parent  # directory where this script is located
    user_swenv_file_json = my_dir / "cpd_software_env.yaml.json"
    if (
        user_swenv_file_json.is_file()
    ):  # shortcut for deployment env which does not have yaml
        import json

        with open(str(user_swenv_file_json)) as fp:
            return (json.load(fp), my_dir)
    user_swenv_file = my_dir / "cpd_software_env.yaml"
    if user_swenv_file.is_file():
        swenv = _read_swenv_yaml(user_swenv_file)
        _my_print_msg(f"read swenv file 'cpd_software_env.yaml'")
        # print(swenv)
        return (swenv, my_dir)
    else:  # by default install anything from subdir cpd_software_env/
        autoinst_dir = Path(my_dir) / "cpd_software_env"
        if autoinst_dir.is_dir():
            # swenv = { "file" : [ str(autoinst_dir)+"/" ]  }
            swenv = {"file": [{"path": "cpd_software_env", "root": str(Path(my_dir))}]}
            print("Implicit swenv from directory", swenv)
    return (swenv, my_dir)


def _normalize_swenv(swenv, swenv_path):
    """Normalize any shorthand notation from the swenv yaml to its expanded form.
    Update entries in swenv dictionary in place."""
    assert isinstance(swenv, dict)
    assert isinstance(swenv_path, Path)

    # swenv_path may be a relative path such as "." or "mydir"
    # make path independent from current work dir
    swenv_path = swenv_path.absolute()

    # todo: handle "module"

    # todo: values of "name" and "base" must be plain strings

    # normalize "file" to "files"
    if "file" in swenv:
        if "files" in swenv:
            print(
                "Error: you can use either key 'file' or 'files' but not both together."
            )
            assert False
        swenv["files"] = swenv.pop("file")

    # normalize "module" to "modules"
    if "module" in swenv:
        if "modules" in swenv:
            print(
                "Error: you can use either key 'file' or 'files' but not both together."
            )
            assert False
        swenv["modules"] = swenv.pop("module")

    # normalize "asset" to "assets"
    if "asset" in swenv:
        if "assets" in swenv:
            print(
                "Error: you can use either key 'asset' or 'assets' but not both together."
            )
            assert False
        swenv["assets"] = swenv.pop("asset")

    # normalize some top-level keys to have lists as values
    for ki in ["files", "assets", "pip"]:  # "module" ?
        if ki in swenv:
            if not isinstance(swenv[ki], list):
                swenv[ki] = [swenv[ki]]  # normalize to list

    # File paths are interpreted relative to the location of the .yaml definition file
    # Convert all paths assuming swenv_path as root directory
    # Also block files that have a path starting with"cpd_software_env"
    files = swenv.get("files", [])
    for idx, item in enumerate(files):
        if isinstance(item, str):
            files[idx] = {"name": item, "path": item, "root": str(swenv_path)}
        else:
            assert isinstance(item, dict)  # todo: error msg
            assert item.get("path")  # must have path for now
            files[idx]["root"] = str(swenv_path / item.get("path", ""))
        if files[idx].get("path").startswith("cpd_software_env") :
            path = files[idx].get("path")
            raise ValueError(f"The prefix 'cpd_software_env' is reserved. Rename file '{path}'")

    # Same for modules
    # File paths are interpreted relative to the location of the .yaml definition file
    # Convert all paths assuming swenv_path as root directory
    files = swenv.get("modules", [])
    for idx, item in enumerate(files):
        if isinstance(item, str):
            files[idx] = {"name": item, "path": item, "root": str(swenv_path)}
        else:
            assert isinstance(item, dict)  # todo: error msg
            assert item.get("path")  # must have path for now
            files[idx]["root"] = str(swenv_path / item.get("path", ""))
        if files[idx].get("path").startswith("cpd_software_env") :
            path = files[idx].get("path")
            raise ValueError(f"The prefix 'cpd_software_env' is reserved. Rename module '{path}'")

    # Assets can be plain path strings. Normalize to dictionary.
    assets = swenv.get("assets", [])
    for idx, item in enumerate(assets):
        if isinstance(item, str):
            if Path(item).suffix == ".py":
                # Watson Studio strips .py suffix after git pull
                assets[idx] = {
                    "name": str(Path(item).stem),
                    "path": item,
                    "asset_type": "script",
                }
            else:
                # ?? remove any parent path?
                assets[idx] = {"name": item, "path": item, "asset_type": "data_asset"}
        else:
            assert isinstance(item, dict)  # todo: error msg
            # todo: normalize type to asset_type
        if assets[idx].get("path").startswith("cpd_software_env") :
            path = assets[idx].get("path")
            raise ValueError(f"The prefix 'cpd_software_env' is reserved. Rename asset '{path}'")

    logging.info(f"Normalized swenv: {swenv}")


def _install_swenv(swenv, swenv_path):
    """Install everything from swenv into the current environment.
    All paths in the swenv are interpreted relative to swenv_path
    """
    # from pathlib import Path # done globally
    assert isinstance(swenv, dict)
    assert isinstance(swenv_path, Path)

    # my_dir = Path(__file__).parent # relative to py module

    for ki in swenv:
        if ki == "pip":
            _install_pip_packages_list(swenv.get("pip"))
        elif ki in [
            "conda",
            "conda_channel",
            "conda_dependencies",
            "channel",
            "dependencies",
        ]:
            print("swenv conda not yet implemented")
        elif ki == "files" or ki == "file":
            # value may be a list or a plain element; map to list in call cases
            file_val = swenv.get("files", swenv.get("file"))
            file_list = file_val if isinstance(file_val, list) else [file_val]
            for p in file_list:
                name_msg = p.get("name", p.get("path")) if isinstance(p, dict) else p
                _my_print_msg(f"set sys.path for '{name_msg}'")
                if isinstance(p, dict):
                    # construct path from dict { path : ...  root: ...}
                    # path is required, root is optional
                    if swenv.get("file_path_ignore_root"):  # e.g. in deployment
                        path = Path(p["path"])
                    else:
                        path = Path(p.get("root", "")) / p["path"]
                    _my_print_msg(f"dict_path {path}")
                else:
                    path = Path(p)
                # print("_install_swenv","swenv_path=",swenv_path,"old path=",path)
                path = swenv_path / path
                if path.is_dir():
                    _install_swenv_dir(path)
                elif path.is_file():
                    _install_swenv_file(path)
                else:
                    print(
                        "Error in install swenv: file",
                        path,
                        "not found. Skipping entry.",
                    )
        elif ki == "modules" or ki == "module":
            print("import module manually in the project")
            # done automatically in deployment
        elif ki == "assets" or ki == "asset":
            asset_list = swenv.get("assets", swenv.get("asset"))
            if swenv.get("assets_added_to_sdist"):
                _provision_assets(asset_list)  # just extend sys.path
            elif _running_in_ws_jupyterlab():
                _my_print_msg("Assuming that assets are already available locally in JupyterLab")
            elif not swenv.get("built_swenv"):
                _my_print_msg("Assuming that assets are already available locally")
            else:
                # Assuming this is running in a deployment space 
                # using an swenv that was built in a project
                _my_print_msg(
                    "Warning: dynamic download of assets in deployment not yet supported"
                )
                _my_print_msg("...      call build(add={'assets'} in the project")
                # Asset download needs env.vars USER_ACCESS_TOKEN and SPACE_ID.
                # These are not predefined in a deployment space runtime.
                # Delay download until a scoring call sets the env.vars.
                global AssetsDownloadPending
                AssetsDownloadPending = asset_list
        elif ki not in [
            "name",
            "base",
            "file_path_ignore_root",
            "assets_added_to_sdist",
            "pip_added_to_sdist",
        ]:
            _my_print_msg(f"swenv tag '{ki}' not recognized, ignoring the value ...")


def _running_in_ws_jupyterlab():
    # hack
    import os

    if os.getenv("JUPYTER_CONFIG_DIR", "").startswith("/home/wsuser/.jupyter/lab"):
        return True
    elif os.getenv("HOSTNAME", "").startswith("jupyter-lab"):
        return True
    return False


def _install_swenv_file(thefile):
    from pathlib import Path
    import sys

    if isinstance(thefile, str) or isinstance(thefile, Path):
        if Path(thefile).suffix == ".py":
            dir = str(Path(thefile).parent)
            if dir not in sys.path:
                sys.path.insert(0, dir)
    else:
        print("Error, not a file:", thefile)


def _install_swenv_dir(autoinst_dir):
    import sys

    # print("_install_swenv_dir", autoinst_dir)
    if autoinst_dir not in sys.path:
        sys.path.insert(0, str(autoinst_dir))
        
    if False: # not needed anymore !?
        autoinst_pipdir = autoinst_dir / "pip"
        if not autoinst_pipdir.is_dir():  # implies path.exists()
            return
        if not any(autoinst_pipdir.iterdir()):  # if empty dir
            print("Warning: There are no files to install from", autoinst_pipdir)
        else:
            if (autoinst_pipdir / "requirements.txt").is_file():
                _install_pip_packages_dir(
                    autoinst_pipdir, autoinst_pipdir / "requirements.txt"
                )
            else:
                # print("No requirements.txt found. Installing all files ...")
                _install_pip_packages_dir(autoinst_pipdir)


def _read_swenv_yaml(filename):
    """Read yaml file and return parsed dictionary"""
    #!pip install pyyaml
    import yaml

    with open(filename) as stream:
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:
            print(exc)
            return {}


def _install_pip_packages_list_old(piplist):
    pip_cmd = ["pip", "install"] + piplist
    _run_pip(pip_cmd)


def _install_pip_packages_list(piplist):
    """Map a list of pip dependencies into a temp. requirements file
    and run pip install.
    """
    import os
    pip_conf = os.path.join(get_path(),"pip.conf")
    if os.path.isfile(pip_conf) :
        _my_print_msg("Setting PIP_CONFIG_FILE="+pip_conf)
        os.environ["PIP_CONFIG_FILE"] = pip_conf

    reqfile_path = _map_pip_packages_list_to_reqfile(piplist)
    with open(reqfile_path) as fp:
        _my_print_msg(reqfile_path + "\n----\n" + fp.read() + "----")
    pip_cmd = ["pip", "install", "-r", str(reqfile_path)]
    _run_pip(pip_cmd)


def _map_pip_packages_list_to_reqfile(piplist):
    """Map a list of pip dependencies into a temp. requirements file.
    Return file name"""
    logging.debug("------ tmp_requirements.txt")
    with open("tmp_requirements.txt", "w") as f:
        for dep in piplist:  # + ["--disable-pip-version-check"]
            if isinstance(dep, dict):
                # handle special option
                if dep.get("path"):
                    fullpath = Path(get_path()) / dep.get("path") / "simple"
                    dep = f"--index-url file://{fullpath}"
                    # pip versions up to v20.x require file:// schema
                    # unfortunately --no-index is not compatible with --index-url
                    # pip will check default indexes for a newer version of pip
                    # avoid using --disable-pip-version-check in run_pip()
                else:
                    print("Error: pip entry is a dict but the key 'path' is missing")
            assert isinstance(dep, str)
            f.write(f"{dep}\n")
    with open("tmp_requirements.txt", "r") as f:
        logging.debug(f.read())
    return "tmp_requirements.txt"


def _install_pip_packages_dir(pkg_dir, reqfile_path=None):
    """Install pip packages from pkg_dir directory.
    Use list of required packages from reqfile_path if provided
    else install all files in the directory.
    """
    import sys

    sys.stdout.flush()  # flush any previous print output before logging
    # sys.executable not set correctly in function deployments
    # pip_cmd = [sys.executable, "-m", "pip","install","--no-cache","--no-index","--find-links="+str(pkg_dir)]
    pip_cmd = [
        "pip",
        "install",
        "--no-cache",
        "--no-index",
        "--find-links=" + str(pkg_dir),
    ]
    # option --no-index prevents pip from fetching any package from other repos
    if reqfile_path:
        pip_cmd += ["-r", str(reqfile_path)]
    else:
        p = pkg_dir.glob("**/*")
        files = [str(x) for x in p if x.is_file()]
        pip_cmd += files
    logging.debug("Calling _run_pip(...)")
    _run_pip(pip_cmd)


def _run_pip(pip_cmd):
    # todo: take pip arguments without initial "pip"
    import subprocess
    import sys

    _my_print_msg(f"run {pip_cmd}")
    sys.stdout.flush()  # flush any previous print output before logging
    # sys.executable not set correctly in function deployments
    # pip_cmd = [sys.executable, "-m", "pip","install","--no-cache","--no-index","--find-links="+str(pkg_dir)]
    logging.info(str(pip_cmd))
    # ? automatically add "--disable-pip-version-check" ?
    logging.debug("cpdpkg:_run_pip:run ...")
    # run_pip = subprocess.run("pip install --index-url langcountry_pipidx/simple langdetect pycountry", shell=True,check=False, stdout=subprocess.PIPE,
    #                          stderr=subprocess.STDOUT, universal_newlines=True)
    # print("TEST",run_pip.stdout)
    # If passing a single string "cmd args...", shell must be True
    run_pip = subprocess.run(
        pip_cmd,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    logging.debug("cpdpkg:_run_pip:run done")
    logging.debug(run_pip.stdout)
    if run_pip.returncode and int(run_pip.returncode) != 0:
        raise Exception(f"pip install failed: {pip_cmd}", run_pip.stdout)

    # Note that we use the (default) argument check=False
    # so we can read the output even when the pip command fails.
    # With check=True the subprocess.run would immediately raise an exception if the pip fails
    # and we would not get any useful error message when the deployment is created.
    # inst_langdetect.check_returncode() # If returncode is non-zero, raise a CalledProcessError
    # Issue: WML online deployment does not forward the details of CalledProcessError to the WML application.
    # Same issue when the function is used in a job run of a batch deployment
    # We raise a generic Exception instead:


def _provision_assets(asset_list):
    """Download assets from the deployment space if needed and extend sys.path to make sure that Python modules can be imported."""
    import sys, os  # sys.path, getenv

    print("_provision_assets", asset_list)
    assert isinstance(asset_list, list)
    for asset_ref in asset_list:
        # todo: download asset from space if needed
        if os.getenv("SPACE_ID"):
            _my_print_msg(f"Found space id {os.getenv('SPACE_ID')}")
        if os.getenv("USER_ACCESS_TOKEN"):
            _my_print_msg("Found an access token")
        #
        # for script assets make sure they can be found via Python sys.path
        path = asset_ref.get("path")
        if path and asset_ref.get("asset_type") == "script":
            dir_path = str((Path(get_path()) / path).parent)
            if str(path) not in sys.path:
                _my_print_msg(f"Adding path {dir_path} for {asset_ref.get('name')}")
                sys.path.insert(0, dir_path)

                
                
def download_pending_assets():
    global AssetsDownloadPending
    import os
    
    if not AssetsDownloadPending:
        print("No assets to download")
        return 
    assert isinstance(AssetsDownloadPending,list)
    _my_print_msg(f"Downloading assets {AssetsDownloadPending}")
    cpd_conn = {}
    assert os.getenv("USER_ACCESS_TOKEN") , "download assets: missing access token"
    if os.getenv("SPACE_ID") : cpd_conn["space_id"] = os.getenv("SPACE_ID")
    elif os.getenv("PROJECT_ID") : cpd_conn["project_id"] = os.getenv("PROJECT_ID")
    else:
        raise Exception("download assets: missing space id and project id")
    _download_assets(cpd_conn,AssetsDownloadPending,Path(get_path()))
    AssetsDownloadPending = []

    
    

if not PkgInitialized:
    # print("Init",__file__)
    (SoftwareEnv, swenv_path) = default_swenv()
    if SoftwareEnv:
        print("Setting up ", swenv_path)
        _normalize_swenv(SoftwareEnv, swenv_path)
        _install_swenv(SoftwareEnv, swenv_path)
        PkgInitialized = True
    else:
        pass
        # print("No default swenv found. Nothing installed yet.")


####################################################################
#
#
# The part below is only used in the development stage, not needed in deployments
#


def setup(filepath):
    """Read software env definition from file and install packages if used"""
    _set_swenv_from_file(filepath)


def _set_swenv_from_file(fpath):
    global PkgInitialized, SoftwareEnv
    global CurrentSwspecId
    if PkgInitialized:
        print("Error: Software Env is already initialized.")
        return None
    assert not SoftwareEnv  # None or empty dict
    CurrentSwspecId = None
    fpath = Path(fpath)  # standardize on Path()
    SoftwareEnv = _read_swenv_yaml(fpath)
    _my_print_msg(f"set from file: {SoftwareEnv}")
    assert SoftwareEnv
    _normalize_swenv(SoftwareEnv, fpath.parent)
    # not needed: _merge_append_swenv(swenv)  # into global SoftwareEnv
    _install_swenv(SoftwareEnv, fpath.parent)
    PkgInitialized = True


def _add_swenv_from_file(fpath):
    global CurrentSwspecId
    CurrentSwspecId = None
    fpath = Path(fpath)  # standardize on Path()
    swenv = _read_swenv_yaml(fpath)
    _my_print_msg(f"from swenv file: {swenv}")
    _normalize_swenv(swenv, fpath.parent)
    _merge_append_swenv(swenv)  # into global SoftwareEnv
    _install_swenv(swenv, fpath.parent)  # if no conflict


def _add_swenv_inline(swenv_as_string):
    global CurrentSwspecId
    CurrentSwspecId = None
    swenv = read_swenv_inline(swenv_as_string)
    _normalize_swenv(swenv, fpath.parent)
    _merge_append_swenv(swenv)  # into global SoftwareEnv
    _install_swenv(swenv, fpath.parent)  # if no conflict


def __install_swenv_local(x):
    """Install files, e.g. in local dev env, but don't add them to the common env"""
    swenv = read_swenv_from_file(fpath)
    _normalize_swenv(swenv, fpath.parent)
    _install_swenv(swenv, fpath.parent)  # tbd use cwd path
    # not merged into global state


def _merge_append_swenv(add):
    # SoftwareEnv.update(add) not sufficient
    assert isinstance(add, dict)
    for ki in add:
        if SoftwareEnv.get(ki):
            ...  # if "name" : name + " + " + name2
            ...  # if "base": must be the same
            SoftwareEnv[ki] += add[ki]
        else:
            SoftwareEnv[ki] = add[ki]


def save(name=None):
    """Save current software env to /project_data/data_asset/cpd_software_env_saved/
    file named saved_{name}.json
    Saved software environments can be used later in load(...)
    """
    name = get_name(name)  # use default env name if name arg is not provided
    # using "saved_" prefix to make sure that there is no cpd_sofware_env.json
    # or disallow name="cpd_sofware_env*" ?
    if False and name.startswith("cpd_software_env"):
        print("Error: prefix 'cpd_software_env' is reserved. Choose a different name.")
        return None
    if SoftwareEnv:
        if not Path("/project_data/data_asset").is_dir():
            print("Error: Missing directory /project_data/data_asset/")
            return None
        _my_run(f"mkdir -p /project_data/data_asset/cpd_software_env_saved")
        _my_run(
            f"cp -p " + __file__ + " /project_data/data_asset/cpd_software_env_saved/"
        )
        path = "/project_data/data_asset/cpd_software_env_saved/saved_" + name + ".json"
        with open(path, "w") as fp:
            import json

            json.dump(SoftwareEnv, fp)
        _gen_build_script(name)
    else:
        print("Warning: Current software env is empty")


def load(name):
    """Load a software environment definition that was previously stored using save(..)."""
    user_swenv_file_json = (
        "/project_data/data_asset/cpd_software_env_saved/saved_" + name + ".json"
    )
    _set_swenv_from_jsonfile(user_swenv_file_json)


def _set_swenv_from_jsonfile(path):
    import json

    global SoftwareEnv
    user_swenv_file_json = path
    with open(str(user_swenv_file_json)) as fp:
        SoftwareEnv = json.load(fp)


def _gen_build_script(name):
    assert name
    code = f"name='{name}'\n"
    code += """
    import sys
    sys.path.insert(0,"/project_data/data_asset/cpd_software_env_saved")
    import cpd_software_env
    cpd_software_env.load(name)
    cpd_software_env.build(copy={"assets"})
    """.replace(
        "    ", ""
    )
    with open(f"swenv_build_{name}.py", "w") as fp:
        fp.write(code)
    print("Generated file " + f"swenv_build_{name}.py")


def build(copy={"files"}, add=None):
    """Collect files, create internal Python package, 
    Create a Software specification and an Environment in the project.
    * copy and add : subset of {"files", "assets","packages"}
    """
    _build_swspec_environment(copy=copy, add=add)
    # don't return id as user does not need it


def _build_swspec_environment(copy={"files"}, add=None):  # or add=set() ?
    """Collect files, create sdists, create swspec and Environment.
    * copy and add : subset of {"files", "assets","packages"}
    """
    global CurrentSwspecId
    global CurrentSwspecName
    import os

    # params copy and add are similar to Docker commands
    # for now we don't make a difference between copy and add param
    assert isinstance(copy, set)
    assert add is None or isinstance(add, set)
    if add:
        copy = copy | add
        
    supported_groups = {"files","assets","pip","packages"}
    for group in copy:
        if group not in supported_groups:
            raise ValueError(f"Unsupported build option '{group}'. Must be in {supported_groups}")

    wml_client = _wml_connect_env()
    wml_client.set.default_project(os.environ["PROJECT_ID"])

    if not CurrentSwspecId:
        _build_and_cache_swspec(wml_client, copy=copy)
        # sets CurrentSwspecName and CurrentSwspecId
    assert CurrentSwspecName and CurrentSwspecId

    envname = get_name()
    print(f"Creating Environment ‘{envname}'")
    _create_ws_environment(wml_client, envname, CurrentSwspecId)

    return CurrentSwspecId


def _create_ws_environment(cpd_conn, name, swspec_id, version="recreate"):
    """Create new Watson Studio Environment based on software_specification object.
    Return representation of new Environment"""
    #
    from datetime import datetime

    hwspec = _lookup_hardware_spec(cpd_conn, "XXS")
    hwspec_id = hwspec["metadata"]["asset_id"]

    runtime = _lookup_runtime(cpd_conn, "jupyter-py37")
    runtime_id = runtime["metadata"]["guid"]

    new_env = {
        "type": "notebook",
        "name": name,
        "display_name": name,  # required, used in GUI
        "description": datetime.now().strftime("%b%d %H:%M")
        + "\ncreated by cpd_software_env",
        # "runtime_idle_time": 1800000,
        "hardware_specification": {"guid": hwspec_id},  # required
        "software_specification": {"guid": swspec_id},
        "tools_specification": {
            "supported_kernels": [
                {
                    "name": "python37",
                    "language": "python",
                    "version": "3.7",
                    "display_name": "Python 3.7",
                }
            ]
        },
        "runtime_definition": runtime_id,
    }

    assert version == "recreate"
    _delete_assets_environment(cpd_conn, name)
    res = _cpd_rest_request35(cpd_conn, "POST", "/v2/environments", json=new_env)
    return res.json()


def _lookup_hardware_spec(cpd_conn, name):
    """return complete representation
    id is ['metadata']['asset_id']
    """
    # _cpd_search_assets(cpd_conn,'hardware_specification')
    # returns empty list in v3.5 June patch :(
    #
    res = _cpd_rest_request35(cpd_conn, "GET", "/v2/hardware_specifications")
    # return res.json()["resources"][0]
    for hw in res.json()["resources"]:
        if hw["metadata"]["name"] == name:
            return hw  # ['metadata']['guid']
    assert False, f"hardware_specification '{name}' does not exist"
    return None


def _lookup_runtime(cpd_conn, name):
    """return complete representation
    id is ['metadata']['guid']
    """

    res = _cpd_rest_request35(cpd_conn, "GET", "/v2/runtime_definitions")
    for rt in res.json()["resources"]:
        if rt["entity"]["name"] == name:
            return rt  # ['metadata']['guid']
    assert False, f"runtime_definition '{name}' does not exist"
    return None


def _cpd_search_assets_environment(cpd_access_info, name):
    """Environment details"""
    # cpd_util._cpd_search_assets(cpd_conn, "environment", name="...")
    # is metadata only, no reference to hwspec no swspec
    # Instead call specific environments API
    res = _cpd_rest_request35(cpd_access_info, "GET", "/v2/environments")
    return [e for e in res.json()["resources"] if e["metadata"]["name"] == name]


def _delete_assets_environment(cpd_access_info, name):
    """delete environment assets with specified name"""
    asset_type = "environment"
    # _cpd_search_assets(cpd_access_info, asset_type, name=name) # returns [] in v3.5 June ;(
    assets = _cpd_search_assets_environment(cpd_access_info, name=name)
    for asset in assets:
        _cpd_delete_asset(cpd_access_info, asset_type, id=asset["metadata"]["asset_id"])


def _build_swenv():
    """Store the current software env in the current project."""
    print("!! Use _build_swspec_environment instead !!")
    global CurrentSwspecId
    global CurrentSwspecName
    import os

    wml_client = _wml_connect_env()
    wml_client.set.default_project(os.environ["PROJECT_ID"])

    if not CurrentSwspecId:
        _build_and_cache_swspec(wml_client)
        # sets CurrentSwspecName and CurrentSwspecId

    assert CurrentSwspecName and CurrentSwspecId
    return CurrentSwspecId


#todo, make private / delete
def gencode_scoring_deploy(userfunction, to_file=None):
    print("Replace gencode_scoring_deploy by gencode_deployable_function")
    return gencode_deployable_function(userfunction,to_file)

def gencode_deployable_function(userfunction,to_file=None):
    """Generate code that implements a deployable Python function invoking userfunction.
    The userfunction is a function object that refers to a regular Python function 
    in the user's code. 
    The generated deployable function uses the specific WML API so a deployment can be generated.
    The generated function transforms the scoring arguments and calls the userfunction.
    Return generated code as string or write to file if to_file is provided.
    * to_file: optional name of output file
    """
    assert type(userfunction).__name__ == "function"
    fn_name = userfunction.__name__
    fn_module = userfunction.__module__

    template_pre = """# Generated by cpd_software_env\n
#wml_python_function
def score(wml_data):
    import os
    import cpd_software_env  # import auto-installer before other modules
    # install commands are run automatically as part of the import
    
    # An input data argument (usually the first one) may contain environment variables;
    # map them to the local session and download any pending assets.      
    for elem in wml_data['input_data']:
        assert isinstance(elem,dict)
        envdict = elem.get('environment_variables')
        if envdict:
            assert isinstance(envdict,dict)
            for var,val in envdict.items():
                os.environ[var] = val
            cpd_software_env.download_pending_assets()
            
    # access THEMODULE after assets have been downloaded
    from THEMODULE import THEFUNCTION
"""
    code = template_pre.replace("THEFUNCTION", fn_name).replace("THEMODULE", fn_module)

    # A Function deployment can be online or batch. In both cases with inline data.
    code += _gencode_server_call_score_inline_data(userfunction, "wml_data")
    # result is in variable "score_res"

    template_post = """
    # WML expects { 'predictions': [some_dictionary] }
    if isinstance(score_res,dict):
        return { 'predictions': [score_res] }
    else:
        result = {}
        #result['autoinstall msgs'] = cpd_software_env.get_msgs()   
        result['score'] =  score_res
        return { 'predictions': [result] }
"""
    code += template_post

    if to_file:
        with open(to_file, "w") as fp:
            fp.write(code)
        return to_file
    else:
        return code


def _gencode_server_call_score_inline_data(scorefn, wml_data_name):
    """Generate Python code that maps wml input data to the function parameters.
    The input data must be provided inline as {wml_data_name}['input_data']
    Does not work for input data references.
    Includes code for final invocation of the score function.
    * scorefn : user's function object
    * wml_data_name : name of the variable that holds the scoring data in WML format
    For equivalent function handling data references see 
    _gencode_server_call_score_data_ref
    """
    from inspect import signature, Parameter

    # Get all parameters of the function scorefn, including their type annotations
    sig = signature(scorefn)
    # scorefn.__annotations__ # e.g. {'text': str, 'return': str}
    #     does not include parameters that don't have a type annotation
    # scorefn.__code__.co_varnames
    #     has all parameters but also other local variables

    code = f"    # map input data to function\n    # {scorefn.__name__}{sig}\n"
    # Assuming that data args are provided inline. Does not work for input data references.
    code += f"    data = {wml_data_name}['input_data']\n"
    #
    code += "    assert isinstance(data,list), 'input_data must be a list'\n"
    # workaround for zen issue 26776 in inline data
    code += "    if len(data)==1 and 'tail' in data[0]:\n"
    code += "        assert isinstance(data[0]['tail'],list), 'input_data tail must be a list'\n"
    code += "        data += data[0]['tail']\n"
    call_args = []
    i = 0
    for param in sig.parameters.values():
        # type(param.annotation) is <class 'type'>
        # todo: expand support for type hints
        # https://docs.python.org/3/library/typing.html
        # https://docs.python.org/3/library/inspect.html#inspect.Parameter.empty
        if not param.annotation or param.annotation == Parameter.empty :
            getval = f"data[{i}]"
        elif param.annotation.__name__ == "DataFrame":
            code += f"    import pandas as pd\n"  
            getval = (
                f"pd.DataFrame(data[{i}]['values'],columns=data[{i}].get('fields'))"
            )
        else :  #  param.annotation.__name__ in [ "str", "list"] :
            # values other than dataframes are assumed to be passed to scoring as
            # { "fields":"paramname", "values":[paramvalue]}
            getval = f"str(data[{i}]['values'][0])"

        code += f"    if {i}>=len(data) : raise ValueError('Too few arguments in input data for deployed function {scorefn.__name__}')\n"
        #todo: support functions with default arguments, param.default != Parameter.empty
        # https://docs.python.org/3/library/inspect.html#inspect.Parameter.empty
        code += f"    val_{param.name} = {getval}\n"
        call_args.append(f"{param.name}=val_{param.name}")
        i += 1

    args_string = ", ".join(call_args)
    code += f"    scorefn_res = {scorefn.__name__}({args_string})\n"

    if sig.return_annotation.__name__ == "DataFrame":
        code += "    # convert DataFrame to JSON serializable dict\n"
        code += "    score_res = { 'fields':list(scorefn_res.columns), 'values':scorefn_res.values.tolist()}\n"
    else:
        code += "    score_res = scorefn_res\n"

    return code




def _gencode_server_call_score_data_ref(scorefn, wml_data_name):
    """Generate Python code that maps wml input data to the function parameters.
    Reverse operation to _gencode_client_call_score_proxy_data_refX(...)
    The input data must be provided as {wml_data_name}['input_data_references']
    Does not work for inline input data.
    Includes code for final invocation of the score function.
    * scorefn : user's function object
    * wml_data_name : name of the variable that holds the scoring data in WML format
    For equivalent function handling data references see 
    _gencode_server_call_score_inline_data
    """
    from inspect import signature, Parameter

    # Get all parameters of the function scorefn, including their type annotations
    sig = signature(scorefn)
    # scorefn.__annotations__ # e.g. {'text': str, 'return': str}
    #     does not include parameters that don't have a type annotation
    # scorefn.__code__.co_varnames
    #     has all parameters but also other local variables
    indent = " "*8
    code = indent+f"# map input data to function {scorefn.__name__}{sig}\n"
    # Assuming that data args are provided inline. Does not work for input data references.
    # input_data_references may be missing
    code += indent+f"assert isinstance({wml_data_name},dict), 'wml_data must be a dict'\n"
    code += indent+f"data = {wml_data_name}.get('input_data_references',[])\n"
    #
    code += indent+"assert isinstance(data,list),'input_data_references must be a list'\n"   # todo, error check

    call_args = []
    i = 0
    for param in sig.parameters.values():
        # type(param.annotation) is <class 'type'>
        # todo: expand support for type hints
        # https://docs.python.org/3/library/typing.html
        # https://docs.python.org/3/library/inspect.html#inspect.Parameter.empty
        if not param.annotation or param.annotation == Parameter.empty or \
           param.annotation.__name__ in ["dict","DataRef"] :
            getval = f"data[{i}]"
        elif param.annotation.__name__ == "DataFrame":
            assert False, "DataFrame not yet supported for data references"
        elif param.annotation.__name__ == "str" :
            getval = f"data[{i}]['location']['inline']"
        else :  #  param.annotation.__name__ in [ "int", "list"] :
            # values other than dataframes are assumed to be passed to scoring as
            # { "fields":"paramname", "values":[paramvalue]}
            getval = f"eval(data[{i}]['location']['inline]')"      
            
        code += indent+f"if {i}>=len(data) : raise ValueError('Too few arguments in input data for deployed script {scorefn.__name__}')\n"
        #todo: support functions with default arguments, param.default != Parameter.empty
        # https://docs.python.org/3/library/inspect.html#inspect.Parameter.empty
        code += indent+f"val_{param.name} = {getval}\n"
        call_args.append(f"{param.name}=val_{param.name}")
        i += 1

    args_string = ", ".join(call_args)
    code += indent+f"scorefn_res = {scorefn.__name__}({args_string})\n"

    if sig.return_annotation.__name__ == "DataFrame":
        code += indent+"# convert DataFrame to JSON serializable dict\n"
        code += indent+"score_res = { 'fields':list(scorefn_res.columns), 'values':scorefn_res.values.tolist()}\n"
    else:
        code += indent+"score_res = scorefn_res\n"

    return code











# cpd_software_env.gencode_scoring_proxy(hello,"my_score_caller.py")
# gen code that invokes the (remote) deployment with inline data
# afterwards: hello_proxy("Eins, Zwei, Drei")
# cf Pyro https://pyro5.readthedocs.io/en/latest/intro.html
#
def gencode_scoring_proxy(userfunction, use_data_refs=False, add_envvars=True,to_file=None):
    """Generate code that invokes the (remote) deployment
    * add_envvars : pass environment variables to the scoring API"""
    fn_proxy_name = userfunction.__name__ + "_proxy"
    template_pre = """
# generated by cpd_software_env\n
def gen_THEPROXY(cpd_url = None, space_name=None, space_id = None, deployment_name = None, token=None, vault=None,logdir="tmpjoboutput") : 
    3DQUOTE
    * cpd_url : url of CP4D server, default is os.environ["RUNTIME_ENV_APSX_URL"]
                note, this is not the scoring url of the specific deployment
    * space_name : name of deployment space
    * deployment_name : name of the deployment
    * token : CP4D bearer token, default is os.environ["USER_ACCESS_TOKEN"]
    * todo: vault_function : function that return a dict { "userid": id , "password": pw}
    3DQUOTE
    import os
    from datetime import datetime
    import cpd_software_env
    # todo, scoring_url parameter
    cpd_info = {}
    if cpd_url:
        cpd_info["url"] = cpd_url
    elif os.getenv("RUNTIME_ENV_APSX_URL") :
        cpd_info["url"] = os.environ["RUNTIME_ENV_APSX_URL"]
    if token :
        cpd_info["token"] = token
    elif os.getenv("USER_ACCESS_TOKEN") :
        cpd_info["token"] = os.environ["USER_ACCESS_TOKEN"]
    if space_id:
        cpd_info["space_id"] = space_id
        os.environ["SPACE_ID"] = space_id
    elif space_name:
        cpd_info["space_id"] = cpd_software_env._lookup_cpd_space_id(cpd_info["url"], cpd_info["token"], space_name)
        os.environ["SPACE_ID"] = cpd_info["space_id"] # used below when calling the score fn
        
    assert deployment_name
    # cpd_software_env.cpd_lookup_asset(cpd_info,"deployment",name=deployment_name)
    # ... doesn't work, deployments are not assets
    res = cpd_software_env._cpd_rest_request35(cpd_info,"GET",f"/ml/v4/deployments?name={deployment_name}")
    assert res.json()["resources"], f"deployment '{deployment_name}' not found"
    deployment = res.json()["resources"][0]
    deployment_id = deployment["metadata"]["id"]
    #print("deployment = ",deployment_id,deployment)  # debug
    # 'deployed_asset_type': 'py_script'
    # 'deployed_asset_type': 'function',
    if deployment["entity"].get('deployed_asset_type') == 'py_script':
        if not USE_DATA_REFS:
            msg = "Proxy code was generated for a deployed function. But the actual deployment runs a script. Re-generate the code using gencode_scoring_proxy(..,use_data_refs=True)"
            raise ValueError("Mismatch between type of input data and deployed asset",msg)
    if deployment["entity"].get('deployed_asset_type') == 'function':
        if  USE_DATA_REFS:    
            print("Mismatch in input_data vs data refs")
            msg = "Proxy code was generated for a deployed script. But the actual deployment runs a function. Re-generate the code using gencode_scoring_proxy(..,use_data_refs=False)"
            raise ValueError("Mismatch between type of input data and deployed asset",msg)  
            
    if "batch" in deployment["entity"] :
        online_or_batch = "batch"
        if os.getenv('CPD_SOFTWARE_ENV_VERBOSE') : 
            print(datetime.now().strftime('%H:%M:%S'),"Found Batch Job")
        # deployed asset can be script or wml_function
        # todo:
        # script requires use_data_refs=True
        # wml_function requires use_data_refs=False
    else:
        assert "online" in deployment["entity"], "deployment must be either batch or online"
        online_or_batch = "online"
        scoring_url = deployment["entity"]["status"]["online_url"]["url"]
        if os.getenv('CPD_SOFTWARE_ENV_VERBOSE') : 
            print(datetime.now().strftime('%H:%M:%S'),"Found online endpoint",scoring_url)
            
    def _lookup_deployed_asset_href():
        nonlocal deployment
        asset_id = deployment['entity']['asset']['id']
        space_id = deployment['entity']['space_id']
        #tmpasset=cpd_software_env.cpd_lookup_asset(cpd_info,"script",id=asset_id)
        #print("tmpasset href=",tmpasset['href'])
        #print("my href=   ",f"/v2/assets/{asset_id}?space_id={space_id}")
        return f"/v2/assets/{asset_id}?space_id={space_id}"
            
    def submit_online_request(wml_data_submit):
        nonlocal scoring_url
        res = cpd_software_env._cpd_rest_request35(
                    {"url":scoring_url},"POST","",json=wml_data_submit)
        return res.json()
        
    wml_client = cpd_software_env._wml_connect_env()
    wml_client.set.default_space(cpd_info["space_id"])
    # todo: replace wml_client with rest call
    def submit_batch_request(wml_data_submit):
        import time,sys
        nonlocal deployment_id,cpd_info,wml_client,logdir
        jobrun = wml_client.deployments.create_job(
                deployment_id=deployment_id,
                meta_props=wml_data_submit)
        job_id = wml_client.deployments.get_job_uid(jobrun)
        print("Job run started:",wml_client.deployments.get_job_status(job_id)["state"])
        timeout_iterations = 200
        while True :
            status = wml_client.deployments.get_job_status(job_id)
            if status['state'] != 'queued' and status['state'] != 'running' :
                break
            if timeout_iterations <= 0 :
                print("giving up")
                break
            print(".",end="");sys.stdout.flush()
            time.sleep(5)
            timeout_iterations -= 1
        print("") # newline after ...
        details = wml_client.deployments.get_job_details(job_id)
        #print("scoring =",details['entity']['scoring'])
        print("state =",details['entity']['scoring']['status']['state'])
        from cpd_software_env import _job_download_output # todo: make inline
        _job_download_output(wml_client,job_id,job_details=details,local_dir=logdir)
        # Batch deployment of a Py Function returns predictions in job details
        # Batch deployment of a Py Function does not produce a file "result.json"
        # Batch deployment of a Py Script does not return predictions in job details
        # Batch deployment of a Py Script produces a file "result.json" or it has failed
        import os
        fname = os.path.join(logdir,"result.json")
        if details['entity']['scoring']['status']['state'] == "failed":
            # todo, check if any log file is available
            raise Exception("Batch job failed",details['entity']['scoring']['status'])
        elif not ( os.path.isfile(fname) or 'predictions' in details['entity']['scoring'] ) :
            # job failed
            job_error_msg = "Batch result missing : "
            job_error_msg += str(details['entity']['scoring']['status'].get('message','no message'))
            # e.g. message={'text': 'output_data_reference is not provided in the payload'}
            # happens when batch script is called with inline input_data
            raise Exception(job_error_msg)
        #
        assert os.path.isfile(fname) or 'predictions' in details['entity']['scoring'], "Result missing"
        if 'predictions' in details['entity']['scoring'] :
            assert not os.path.isfile(fname) 
        if os.path.isfile(fname) :
            assert 'predictions' not in details['entity']['scoring']  
        if 'predictions' not in details['entity']['scoring'] :
            try:
                import json,os
                #print("result file? ",os.path.join(logdir,"result.json"))
                fp = open(os.path.join(logdir,"result.json"))
                #print("found",os.path.join(logdir,"result.json"))
                try:
                    score_res = json.load(fp)
                    fp.close()
                except Exception as exc:
                    print("Error loading JSON")
                    score_res = str(exc)
                # cf gencode_deployable_function  template_post
                # WML expects { 'predictions': [some_dictionary] }
                if isinstance(score_res,dict):
                    result = score_res
                else:
                    result = {}
                    #result['autoinstall msgs'] = cpd_software_env.get_msgs()   
                    result['score'] =  score_res
            except Exception as exc:
                print("Warning: no result.json found")
                result = str(exc)
            if os.getenv('CPD_SOFTWARE_ENV_VERBOSE') : 
                print(datetime.now().strftime('%H:%M:%S'),'batch result',result)
            details['entity']['scoring']['predictions'] = [result]
        #
        #print("predictions",details['entity']['scoring']['predictions'])
        if details['entity']['scoring']['status']['state'] == "failed":
            raise Exception("submit_batch_request",details['entity']['scoring']['status'])
        return details['entity']['scoring']
        # returns a dictionary with key 'predictions', consistent with result from online scoring
  
"""
    code = template_pre.replace("THEPROXY", fn_proxy_name).replace("3DQUOTE", '"""').replace("USE_DATA_REFS",str(use_data_refs))

    code += _gencode_client_call_score_proxy(userfunction,use_data_refs=use_data_refs,add_envvars=add_envvars)

    if to_file:
        with open(to_file, "w") as fp:
            fp.write(code)
        return to_file
    else:
        return code

# todo, get *latest* deployment in case there are duplicate names
# see code in WML_Dev/deployment-url.ipynb
    
# Calling Py Function with dataa references causes error:
#  Status code: 400, body: {"trace":"a70ea465fbb9aa2dc4e1c07a2b69f4ef","errors":[{"code":"unsupported_async_payload","message":"Unsupported Payload for Async Scoring Found. ai-function is not supported for Async Scoring with reference on local platforms.","target":{"type":"json","name":"payload.scoring_specs.input_data.reference"}}]}    
    
    
    
    
    
    
    
def _gencode_client_call_score_proxy(userfunction,use_data_refs,add_envvars):
    """Generate code that calls an online or batch scoring endpoint.
    The generated code will define a Python function
        def userfunction_proxy(param1:type1, param2:type3, ...) -> type_res
    that has the same parameters and type annotation as the original userfunction.
    The proxy function maps the parameters to objects as required by the WML API.
    The code will submit the data as inline data with key 'input_data'
    or as data references when use_data_refs.
    """
    from inspect import signature

    fn_proxy_name = userfunction.__name__ + "_proxy"
    sig = signature(userfunction)
    
    #if len(signature(userfunction).parameters)>1:  # zen issue 26776
        #print("Warning: CP4D 3.5 does not support multiple input data sets for Python Function online deployments")
        # WML will take an "input_data" list with multple entries as input payload
        # but only the first entry will be passed to the user's score function.
        # Batch Scoring works fine.
        # cpd_software_env uses a workaround for online scoring

    code = ""
    code += "    import pandas\n"  # just in case
    code += "    def " + fn_proxy_name + str(sig) + ":\n"
    code += "        nonlocal online_or_batch\n"
    # online_or_batch is a str that is either "online" or "batch"
    indent = " " * 8
    
    if add_envvars:
        # todo, make code dynamic, lookup values of vars in every call
        # add credential to the scoring arguments 
        # so the server side can look up assets from it's deployment space
        code += indent + "import os\n"
        code += indent + "envvars={}\n"
        # Use current values of env.vars as they may have been updated, e.g. with a fresh token
        for v in ['USER_ACCESS_TOKEN','SPACE_ID'] : 
            # skip 'RUNTIME_ENV_APSX_URL', use internal url instead
            code += indent + f"if os.getenv('{v}'): envvars['{v}']=os.environ['{v}']\n"
        # wml_client.deployments.ScoringMetaNames.ENVIRONMENT_VARIABLES == "environment_variables"
    
    envvars_name = ("envvars" if add_envvars else "")
    if use_data_refs:
        code += _gencode_client_call_score_proxy_data_refX(sig,envvars_name,"wml_data")
    else:
        code += _gencode_client_call_score_proxy_inline_dataX(sig,envvars_name,"wml_data")
    # generated code assigns the WML data structure to the variable "wml_data"
    

    code += "        if os.getenv('CPD_SOFTWARE_ENV_VERBOSE') : print(datetime.now().strftime('%H:%M:%S'),'wml_data',wml_data)\n"

    code += "        if online_or_batch == 'online' :\n"
    code += "            wml_result = submit_online_request(wml_data)\n"
    code += "        else :\n"    
    code += "            wml_result = submit_batch_request(wml_data)\n"    
    code += "        if os.getenv('CPD_SOFTWARE_ENV_VERBOSE') : print(datetime.now().strftime('%H:%M:%S'),'wml_result',wml_result)\n"
    code += "        # todo: make sure that batch logs get downloaded before any other error/exception exits the app\n"

    code += "        if not isinstance(wml_result,dict): return wml_result\n" # return error?
    code += "        if 'predictions' not in wml_result: return wml_result\n" # return error?

    if sig.return_annotation.__name__ == "DataFrame":
        code += "        import pandas as pd\n"
        code += '        d = wml_result["predictions"][0]\n'
        code += '        return pd.DataFrame(d["values"],columns=d.get("fields",[]))\n'
    elif sig.return_annotation.__name__ == "str":
        code += '        return wml_result["predictions"][0]["score"]\n'
    else:
        code += '        return wml_result["predictions"][0]\n'

    code += "\n    return " + fn_proxy_name
    return code


# user's script file can have
# if __name__ ==  "__main__":
#     ... map sys.argv to my_main parameters
#     my_main(...)
# to make it compatible with WStudio platform jobs
#
# cpd_software_env imports my_main but does not set __name__ to "__main__"

# A deployed script can only be used in a batch deployment
# Script in an online deployment is not supported by WML
#
# A batch deployment of a script requires input data references.
# Calling a batch script deployment with inline input_data will fail.


# from wml utilities
def _job_download_output(client,job_id,job_details=None,local_dir="tmpjoboutput") :
    def _get_uid_from_href(href) :
        return href.split('?')[0].split('/')[-1]

    # Note, scoring.output_data_reference.location might not have an href attribute.
    # This can happen when WML did not write any output data asset, e.g. 
    # because the script did not create any files in $BATCH_OUTPUT_DIR

    if not job_details:
        job_details = client.deployments.get_job_details(job_id)

    outdata_href = None
    assert job_details['entity']['scoring'], "Invalid job details object"
    assert isinstance(job_details['entity']['scoring'],dict), "Invalid job details object"
    # job might have run with inline data, there would be no output_data_reference in that case
    if 'output_data_reference' in job_details['entity']['scoring'] :
        outdata_loc = job_details['entity']['scoring']['output_data_reference']['location']
        outdata_href = outdata_loc.get('href')

    if outdata_href :
        import shutil, zipfile
        #shutil.rmtree( local_dir , ignore_errors=True) to be done by caller

        outdata_id = _get_uid_from_href(outdata_href)
        zipdown = client.data_assets.download(outdata_id,filename=local_dir+".zip")
        with zipfile.ZipFile(local_dir+".zip", 'r') as zip_ref:
            zip_ref.extractall(local_dir)
        return local_dir
    else :
        print('no output zip available')
        return None







def _gencode_client_call_score_proxy_inline_dataX(sig,envvars_name,wml_data_name):
    """Given the signature of a Python function, generate code that maps the
    Python parameter values to a WML inline data structure {'input_data': [...]}
    Optionally include environment variables.
    * envvars_name : name of the Python variable containing an env.var dictionary.
    * wml_data_name : name of the Python variable to be set in the code.
    """
    from inspect import signature, Parameter
    
    code =""
    data_list = []  # to be submitted as "input_data":
    n=1
    for param in sig.parameters.values():
        if not param.annotation or param.annotation == Parameter.empty :
            code += f"        assert isinstance({param.name},dict),'arg must be a dict'\n"
            #code += f"        assert 'fields' in {param.name}\n"
            code += f"        assert 'values' in {param.name},'values missing'\n"
            code += f"        d{n} = {param.name}\n"  
        elif param.annotation.__name__ == "DataFrame":
            fields_expr = f"list({param.name}.columns)"
            values_expr = f"{param.name}.values.tolist()"
            code += f"        d{n} = {{ 'fields':{fields_expr}, 'values':{values_expr} }}\n"
        else :   # if param.annotation.__name__ in [ "str", "list"] :
            code += f"        d{n} = {{ 'fields':['{param.name}'], 'values':[{param.name}] }}\n"
            # "values" must have a list (for batch scoring)    
        #   
        # Set 'name' or 'id' to name of the parameter
        code += f"        d{n}['name'] = '{param.name}'\n"
        data_list.append(f"d{n}")
        n+=1

    if data_list == []:  # at least one element is required by wml !?
        # construct a dummy data entry
        code += f"        d1 = {{ 'fields':['dummy'], 'values':[[]] }}\n"
        data_list.append("d1")

    if envvars_name:
        code += f"        d1['environment_variables'] = {envvars_name}\n"

    code += "        data_list = [" + ",".join(data_list) + "]\n"
    
    #if len(data_list) > 1:
        # zen issue 26776
        # Issue in WML, the implementation of online scoring passes only the first
        # input_data element to the Python function; the other elements in the list are ignored.
        # work-around: link them into the first element
    assert len(data_list) >= 1
    code += "        data_list[0]['tail'] = data_list[1:]\n"
    
    # finally
    code += "        "+wml_data_name + " = { 'input_data': data_list}\n"
    return code


# base64 encoded "content"
# import base64
# encodedBytes = base64.b64encode(data.encode("utf-8"))
# "input_data": [ {"content" : str(encodedBytes, "utf-8"), ...} ]



def _gencode_client_call_score_proxy_data_refX(sig,envvars_name,wml_data_name):
    """Given the signature of a Python function, generate code that maps the
    Python parameter values to a WML structure {'input_data_references': [...]}
    Optionally include environment variables.
    Reverse operation to _gencode_server_call_score_data_ref(...)
    ? script_name : name of script asset (in deployment space), required for href
    * envvars_name : name of the Python variable containing an env.var dictionary.
    * wml_data_name : name of the Python variable to be set in the code.
    """
    # Using WML constants as plain string literals
    # wml_client.deployments.ScoringMetaNames.ENVIRONMENT_VARIABLES
    # wml_client.deployments.ScoringMetaNames.INPUT_DATA_REFERENCES
    # wml_client.deployments.ScoringMetaNames.OUTPUT_DATA_REFERENCE
    from inspect import signature, Parameter
    
    code =""
    code += "        dummy_href = _lookup_deployed_asset_href()\n"
    
    data_list = []  # to be submitted as "input_data_references":
    n=1
    for param in sig.parameters.values():
        if not param.annotation or param.annotation == Parameter.empty or \
           param.annotation.__name__ in ["dict","DataRef"]:
            #print("gencode proxy: data ref")
            code += f"        assert isinstance({param.name},dict),'arg must be a dict'\n"
            #code += f"        assert 'type' in {param.name}\n"  # ?
            code += f"        assert 'location' in {param.name},'location missing'\n"
            # cpd_software_env requires location to have 'name' key,  todo check ValueError
            code += f"        d{n} = {param.name}.copy()\n"  
            # href key in location required by WML
            code += f"        if 'href' not in d{n}['location'] : \n"
            code += f"            d{n}['location']['href'] = dummy_href\n" 
        elif param.annotation.__name__ == "DataFrame":
            # todo: error msg
            assert False, "DataFrame not yet supported for input_data_references"
            # use data ref instead
        else :   # if param.annotation.__name__ in [ "str", "list"] :
            #print("gencode proxy: type",param.annotation.__name__ )
            # use 'content' : base64-string? 
            # https://dataplatform.cloud.ibm.com/docs/content/DO/WML_Deployment/ModelIODataDefn.html
            code += f"        d{n} = {{ 'location': {{ 'inline':str({param.name}), 'href':dummy_href }} }}\n"
        #      
        # Set 'name' or 'id' to name of the parameter. However, see zen issue 26877
        code += f"        d{n}['name'] = '{param.name}'\n"
        code += f"        if 'type' not in d{n}:\n"
        code += f"            d{n}['type'] = 'data_asset'\n"   # 'type' key required by WML
        # WML Cloud uses type 'connection_asset' for connected data asset !?
        data_list.append(f"d{n}")
        n+=1

    code += "        data_list = [" + ",".join(data_list) + "]\n"
    code += "        outdata = {'type': 'data_asset', 'location': { 'name': 'deploy_test_script-out' }}\n"
    
    # finally
    code += "        "+wml_data_name + " = {\n" 
    if envvars_name:
        code += f"                'environment_variables': {envvars_name},\n"
    if data_list:
        code += "                'input_data_references': data_list,\n"
        # 'input_data_references' : [] would cause error "list index out of range"
    code += "                'output_data_reference': outdata }\n"
    return code


# to be used when creating a WML job
#     wml_params = {
#        wml_client.deployments.ScoringMetaNames.ENVIRONMENT_VARIABLES: 
#            { "URL" : ..., "SPACE_ID" : ... },
#        wml_client.deployments.ScoringMetaNames.INPUT_DATA_REFERENCES: [{
#            'type': 'data_asset',
#            'location': { 'href': ... },
#            'name' : 'myname1', 
#        },
#        {   ...  },
#        {
#            'type': 'data_asset',
#            'location': { 'href': input_data2_asset_href , 
#                          "inline_value":str({"mylockey":99})},  # must be string valuue
#        }],
#        wml_client.deployments.ScoringMetaNames.OUTPUT_DATA_REFERENCE: {
#            'type': 'data_asset',
#            'location': { 'name': 'deploy_test_script-out' }
#        },
#
#    }
#    job = wml_client.deployments.create_job(deployment_id, meta_props=wml_params)



#todo, make private / delete
def gencode_scoring_deploy_batch(userfunction, to_file=None):
    print("Replace def gencode_scoring_deploy_batch by gencode_deployable_script!")
    return gencode_deployable_script(userfunction, to_file)


def gencode_deployable_script(userfunction, to_file=None):
    """Generate code that implements a deployable Python batch script invoking userfunction.
    Return as string or write to file if to_file is provided.
    * to_file: optional name of output file
    """
    assert type(userfunction).__name__ == "function", "userfunction must be a Python function object"
    fn_name = userfunction.__name__
    fn_module = userfunction.__module__

    template = """
# generated by cpd_software_env\n
if __name__ == "__main__" :
    import shutil,json
    import os,sys
    import traceback, logging

    
    # Preamble setting up logging
    #
    # The Job log in WML (CP4D v3) does not include stdout or stderr
    # Therefore, write tracing info into a file in $BATCH_OUTPUT_DIR
    #
    output_dir = os.getenv('BATCH_OUTPUT_DIR')
    if output_dir :
        logfile=os.path.join(output_dir,"system-logging.log")
        logging.basicConfig(filename=logfile,level="DEBUG",
                        format='%(asctime)s %(levelname)-7s %(message)s',
                        datefmt='%H:%M:%S')  
        # https://docs.python.org/3/library/logging.html#levels
    else:
        logging.basicConfig(stream=sys.stdout, level="INFO") # level=logging.DEBUG)
    #
    logging.info("logging started")
    if output_dir and os.getenv('JOBS_PAYLOAD_FILE') :
            shutil.copy(os.environ['JOBS_PAYLOAD_FILE'],output_dir)
            
    #sys.exit(0)  ### DEBUG

    try:
        
        outdir = os.getenv("BATCH_OUTPUT_DIR",".")
        logfile = os.path.join(outdir,"system-logging.log")

    
        # workaround for zen issue 19184
        # https://github.ibm.com/PrivateCloud-analytics/Zen/issues/19184
        #import sys
        #sys.path.append("/opt/ibm/scoring/python/cust-libs")
        
        logging.info("try import cpd_software_env")
        try:
            import cpd_software_env
            logging.info("cpd_software_env available")
            with open(logfile,"a") as fp:
                pkgpath = cpd_software_env.get_path()
                fp.write(f"cpd_software_env path = '{pkgpath}'\\n")
                fp.write("cpd_software_env messages:\\n")
                fp.write(str(cpd_software_env.get_msgs()))
                fp.write("\\n")
        except ModuleNotFoundError:
            logging.warning("No package cpd_software_env found.")

        
        # import the main function of my batch program
        logging.info("import user module")
        from THEMODULE import THEFUNCTION
        logging.info("user module available")
        

        payload = None
        # get custom input parameters for this batch job
        if os.getenv('JOBS_PAYLOAD_FILE') :
            with open(os.environ['JOBS_PAYLOAD_FILE'],"r") as json_file :
                payload = json.load(json_file)  
        
        logging.info("Calling my_batch_main")
        with open(logfile,"a") as fp:
            fp.write("Calling THEFUNCTION:"+str(payload)+"\\n")
        sys.stdout = open(os.path.join(outdir,"user-stdout.log"),"w")
        
        # THEFUNCTION(payload)  # ...

"""
    code = template.replace("THEFUNCTION", fn_name).replace("THEMODULE", fn_module)
    
    code += _gencode_server_call_score_data_ref(userfunction, "payload['scoring']")
    # result will be available in variable score_res
    
    code += """
        #sys.stdout.close()
        logging.info("Batch script finished")
        #logging.debug(str(score_res))
        resultfile = os.path.join(outdir,"result.json")
        with open(resultfile,"w") as fp:
            import json
            json.dump(score_res, fp)
        #todo: parse into result type by client proxy

    except Exception as ex:
        logging.info("Batch script terminated with an Exception")
        logging.error(traceback.format_exc())
        # re-raise to WML server so the exception appears in the WML log
        # bare raise to re-raise the exception currently being handled)
        # TODO: add option "catchall" to make sure that log files are returned to user
        #raise  # forwarding previous exception
        # Problem: If the deployed script returns an exception then WML won't upload
        # the logging files as a data asset (zip file)
        # Instead, write "error file"
        errorfile = os.path.join(outdir,"ERROR.txt")
        with open(errorfile,"w") as fp:
            fp.write(str(traceback.format_exc()))
        
            
    finally:
        logging.info("shutdown")
        logging.shutdown()
"""
    
    if to_file:
        with open(to_file, "w") as fp:
            fp.write(code)
        return to_file
    else:
        return code
  
    
# Inline data can also be used for batch deployments of Functions
# complete example in WML_Pub/PyFunction_Hello_Test.ipynb
batch_inline_call = {
  "deployment": {
    "id": "5c46a8d3-3ef0-4c49-b8a0-5277eadb1377"
  },
  "hardware_spec": {
    "id": "f3ebac7d-0a75-410c-8b48-a931428cc4c5"
  },
  "platform_job": {
    "job_id": "40f23d9e-512f-4c50-82aa-d6f5c3460666",
    "run_id": "501f1620-3d48-466d-9acb-ace2dc3e4b50"
  },
  "scoring": {
    "input_data": [
      {
        "fields": [],
        "values": [
          [
            1
          ]
        ]
      }
    ],
    "status": {
      "completed_at": "",
      "running_at": "",
      "state": "queued"
    }
  }
}
    
    
    
    
    

def save_script_to_project(script_path, name=None,sw_spec_name="__swenv__"):
    """Store the Python script as an asset in the current project.
    Use the current software env as software specification for the script.
    Override by setting sw_spec_name=None. This avoids triggering a build()."""
    global CurrentSwspecId
    global CurrentSwspecName
    import os

    if not script_path:
        raise ValueError("save_script_to_project : missing path to script file")
    if not name:
        name = os.path.basename(script_path)
        
    wml_client = _wml_connect_env()
    wml_client.set.default_project(os.environ["PROJECT_ID"])

    if sw_spec_name == "__swenv__":
        if not CurrentSwspecId:
            _build_and_cache_swspec(wml_client)
            # sets CurrentSwspecName and CurrentSwspecId
        assert CurrentSwspecName and CurrentSwspecId
        sw_spec_name = CurrentSwspecName

    script_id = _save_python_script(
        wml_client,
        name,
        resource=script_path,
        sw_spec_name=sw_spec_name,
        version="replace",
    )

    print(f"Script '{name}' saved to project")
    return script_id


def save_function_to_project(score_fn, name=None):
    """Store the Python function as an asset in the current project.
    Use the current software env as software specification for the function."""
    global CurrentSwspecId
    global CurrentSwspecName
    import os

    wml_client = _wml_connect_env()
    wml_client.set.default_project(os.environ["PROJECT_ID"])

    if not CurrentSwspecId:
        _build_and_cache_swspec(wml_client)
        # sets CurrentSwspecName and CurrentSwspecId

    assert CurrentSwspecName and CurrentSwspecId
    fn_name = name if name else score_fn.__name__  # or score.__module__ ?
    function_asset_id = _save_python_function(
        wml_client, score_fn, fn_name, sw_spec_name=CurrentSwspecName, version="replace"
    )
    print(f"Function '{fn_name}' saved to project")
    return function_asset_id


def save_model_to_project(pipeline_model, name):
    """Store the model as an asset in the current project.
    Use the current software env as software specification for the model.
    TODO: Expand to model types other than sklearn."""
    # todo: optional param for training data
    global CurrentSwspecId
    global CurrentSwspecName
    import os

    wml_client = _wml_connect_env()
    wml_client.set.default_project(os.environ["PROJECT_ID"])

    if not CurrentSwspecId:
        _build_and_cache_swspec(wml_client)
        # sets CurrentSwspecName and CurrentSwspecId

    assert CurrentSwspecName and CurrentSwspecId

    model_type = "scikit-learn_0.23"  # todo, derive from model

    version = "replace"  # todo, delete old versions as in save_function

    meta_props = {
        wml_client.repository.ModelMetaNames.NAME: name,
        wml_client.repository.ModelMetaNames.TYPE: model_type,
        wml_client.repository.ModelMetaNames.SOFTWARE_SPEC_UID: CurrentSwspecId,
    }
    model_info = wml_client.repository.store_model(
        pipeline_model, meta_props=meta_props
    )
    model_uid = wml_client.repository.get_model_uid(model_info)
    return model_uid


#
# Create a CP4D software_specification object.
#
# Step 1: Wrap the user files into a Python "source distribution" package
# Step 2: Create a corresponding software_specification


# not used?
def __cached_swspec_id():
    global CurrentSwspecId
    if not CurrentSwspecId:
        CurrentSwspecId = _build_and_cache_swspec()
    return CurrentSwspecId


# renamed from wrap_into_swspec
def _build_and_cache_swspec(wml_client=None, custom_sw_spec_name=None, copy={"files"}):
    """Wrap all sw env files together into a Python software distribution package
    and create a new CPD/WML software_specification object
    """
    global SoftwareEnv
    global CurrentSwspecId
    global CurrentSwspecName

    if not wml_client:
        wml_client = _wml_connect_env()
        wml_client.set.default_project(os.environ["PROJECT_ID"])

    (swspec_name, swspec_id) = _build_swspec_wml(
        wml_client, custom_sw_spec_name, copy=copy
    )

    CurrentSwspecId = swspec_id
    CurrentSwspecName = swspec_name
    return swspec_id


def get_name(custom_name=None):
    """Get the name of the current software env.
    The name is derived from
    1. name key in the software definition
    2. name of the file from which the swenv was set up
    3. project name
    4. "cpd_software_env"
    """
    global SoftwareEnv
    assert not custom_name, "parameter is deprecated"
    if custom_name:
        return custom_name
    assert SoftwareEnv
    assert isinstance(SoftwareEnv, dict)
    if SoftwareEnv.get("name"):
        return SoftwareEnv.get("name")
    # name of the file from which the swenv was set up, is mapped to name key during setup
    return os.getenv("PROJECT_NAME", "cpd_software_env")


def _build_swspec_wml(wml_client, custom_sw_spec_name=None, copy={"files"}):
    """Build software dist file and create sw_spec and save into wml_client"""
    # todo: refactor
    import os

    global SoftwareEnv

    _my_print_msg(f"_build_swspec_wml(.., {custom_sw_spec_name}, copy={copy} )")

    if not SoftwareEnv:
        (SoftwareEnv, swenv_path) = default_swenv()
        _normalize_swenv(SoftwareEnv, swenv_path)
    assert SoftwareEnv
    assert isinstance(SoftwareEnv, dict)

    base_swenv_name = SoftwareEnv.get("base", "default_py3.7_opence")
    assert base_swenv_name
    assert isinstance(base_swenv_name, str)
    _my_print_msg(f"base swpec = {base_swenv_name}")

    swenv_name = get_name(custom_sw_spec_name)
    assert swenv_name
    assert isinstance(swenv_name, str)

    # wrap all sw env files together into a Python software distribution package
    autoinstall_sdist_file = _create_autoinstall_sdist(copy=copy)
    modules_sdist_files = []
    if SoftwareEnv.get("module"):
        modules = SoftwareEnv.get("module")
        assert isinstance(modules, list)
        assert len(modules) == 1
        module_sdist_file = _create_modules_sdist(modules)
        modules_sdist_files.append(module_sdist_file)

    # create a CPD/WML software_specification object
    # issue: GUI does not provide "delete" action for swspecs
    # workaround: make names unique
    from datetime import datetime

    swspec_name = swenv_name + datetime.now().strftime(" %b%d %H:%M")
    swspec_id = _create_software_specification(
        wml_client,
        base_swenv_name,
        [autoinstall_sdist_file] + modules_sdist_files,
        custom_name=swspec_name,
        version="replace",  # todo: implement "patch" option
    )
    return (swspec_name, swspec_id)


def _create_autoinstall_sdist(copy={"files"}):
    """
    The autoinstall package will include
    * this py file
    * all files from the subdirectory cpd_software_env
    """
    import logging
    import shutil
    import subprocess
    from pathlib import Path

    global SoftwareEnv

    if not SoftwareEnv:
        (SoftwareEnv, swenv_path) = default_swenv()
    assert SoftwareEnv

    # build tree for sdist will contain:
    # /tmp/MyDistPkg/setup.py
    #                MANIFEST.in
    #                cpd_software_env/__init__.py
    #                cpd_software_env/cpd_software_env.py
    #                cpd_software_env/cpd_software_env/  # link to user's pkg_dir
    #                dist/cpd_software_env-0.1.zip   # built sdist

    # temporary directory for building the sdist package
    tmp_dir = "/tmp"
    build_dir = f"{tmp_dir}/MyDistPkg"
    shutil.rmtree(f"{tmp_dir}/MyDistPkg", ignore_errors=True)
    # _my_run(f"rm -rf {tmp_dir}/MyDistPkg")

    # copy package files to sdist build dir

    _my_run(f"mkdir -p {build_dir}/cpd_software_env")
    _my_run(f"cp {__file__} {build_dir}/cpd_software_env/cpd_software_env.py")

    swenv_in_build = SoftwareEnv.copy()

    # set up data files to include in sdist
    _create_autoinstall_sdist_manifest(swenv_in_build, f"{build_dir}", copy)
    # might update swenv_in_build

    swenv_in_build["file_path_ignore_root"] = True
    swenv_in_build["built_swenv"] = True  # this swenv is built from another swenv
    # saving sw env definition as json instead of yaml
    # so we don't depend on yaml to be installed in deployment env
    with open(f"{build_dir}/cpd_software_env/cpd_software_env.yaml.json", "w") as fp:
        import json

        json.dump(swenv_in_build, fp)

    _my_write_text(
        f"{build_dir}/cpd_software_env/__init__.py",
        ["from .cpd_software_env import *", "__version__ = '0.1'"],
    )

    _my_write_text(
        f"{build_dir}/setup.py",
        [
            "from setuptools import setup, find_packages",
            "setup(",
            "    name='cpd_software_env',",
            "    version='0.1',",
            "    packages=find_packages(),",
            "    include_package_data=True)",
        ],
    )

    _my_run(f"(cd {build_dir} && rm -rf ./dist && python setup.py sdist --formats=zip)")

    sdist_file = f"{build_dir}/dist/cpd_software_env-0.1.zip"
    logging.info(f"created sdist {sdist_file}")
    return sdist_file


def _create_autoinstall_sdist_manifest(swenv, build_dir, copy={"files"}):
    """Set up data files to include in sdist.
    For files referenced in swenv include them in {build_dir}/MANIFEST.in"""
    # build_dir can be any (temp) directory
    # swenv might refer to a file with { "root" : localprefix , "path" : thepath }
    # absolute_local_path is dir(swenv_file) / localprefix / thepath
    # 1. ln -s {absolute_local_path} {build_dir}/cpd_software_env/{thepath}
    # 2. *include cpd_software_env/{thepath}
    #
    # https://packaging.python.org/guides/using-manifest-in/
    from pathlib import Path

    print(
        f"autoinstall:_create_autoinstall_sdist_manifest(...,{build_dir}, copy={copy})"
    )
    logging.info(
        f"autoinstall:_create_autoinstall_sdist_manifest(...,{build_dir}, copy={copy})"
    )
    my_dir = Path(__file__).parent
    manifest_lines = []
    add = copy

    if add and (add & {"file", "files"}):
        # add files as per definition of the swenv
        _my_print_msg("Adding files to sdist")
        # todo: def _add_files_to_sdist(file_list,my_dir,build_dir)
        for p in swenv.get("files", swenv.get("file", [])):
            logging.debug(f"sdist add file {p}")
            if isinstance(p, dict):
                local_prefix = p.get("root", "")
                path = p["path"]
            else:
                assert isinstance(p, str)
                local_prefix = ""
                path = Path(p)
            local_path = my_dir / local_prefix / path
            # print("autoinstall:_create_autoinstall_sdist_manifest include",local_path)
            if (
                str(path) == "." or str(path) == "./"
            ):  # current dir needs special handling
                # for each file/dir in local_path link file as with local_path.is_file()
                # print("Current Dir !")
                for each_path in local_path.iterdir():
                    # print(each_path,each_path.name,each_path.is_file(),each_path.is_dir())
                    fname = each_path.name
                    # if fname in [".ipynb_checkpoints","__pycache__"]:
                    #    continue  # handled in manifest
                    if each_path.is_dir():
                        _my_run(
                            f"ln -s {each_path.absolute()} {build_dir}/cpd_software_env/{fname}"
                        )
                        manifest_lines.append(
                            f"recursive-include cpd_software_env/{fname} *"
                        )
                    elif each_path.is_file():
                        _my_run(
                            f"ln -s {each_path.absolute()} {build_dir}/cpd_software_env/"
                        )
                        manifest_lines.append(f"include cpd_software_env/{fname}")
                    else:
                        assert False  # something wrong with iterdir()
            elif local_path.is_dir():
                _my_run(
                    f"ln -s {local_path.absolute()} {build_dir}/cpd_software_env/{path}"
                )
                manifest_lines.append(f"recursive-include cpd_software_env/{path} *")
            elif local_path.is_file():
                _my_run(f"ln -s {local_path.absolute()} {build_dir}/cpd_software_env/")
                manifest_lines.append(f"include cpd_software_env/{path}")
            else:
                print("Error in sdist swenv: file", path, "not found. Skipping entry.")

    if add and (add & {"asset", "assets"}):
        _my_print_msg("Adding assets to sdist")
        cpd_conn = {"project_id": os.environ["PROJECT_ID"]}
        asset_list = swenv.get("assets", swenv.get("asset", []))
        ml = _add_assets_to_sdist(cpd_conn, asset_list, my_dir, Path(build_dir))
        manifest_lines += ml
        swenv["assets_added_to_sdist"] = True  # indicator used later in provisioninig
        assert swenv.get("assets_added_to_sdist")

    if add and (add & {"pip", "packages"}):
        _my_print_msg("Adding pip packages to sdist")
        cpd_conn = {"project_id": os.environ["PROJECT_ID"]}
        pip_list = swenv.get("pip", [])
        if pip_list:
            ml = _add_pip_to_sdist(cpd_conn, pip_list, my_dir, Path(build_dir))
            manifest_lines += ml
            # insert special option "path" to pip list
            # use reserved path to avoid conflict with user paths
            swenv["pip"] = [{"path": "cpd_software_env_pipindex/"}] + swenv["pip"]
            swenv["pip_added_to_sdist"] = True  # indicator used later in provisioninig

    _my_write_text(
        f"{build_dir}/MANIFEST.in",
        manifest_lines
        + [
            "include cpd_software_env/cpd_software_env.yaml",
            "include cpd_software_env/cpd_software_env.yaml.json",
            "include cpd_software_env/requirements.txt",  # ?
            "global-exclude __pycache__/*",
            "global-exclude .ipynb_checkpoints/*",
            "global-exclude .DS_Store",
        ],
    )
    # todo, exclude bytecode


# .gitingore
# *.py[cod]      # Will match .pyc, .pyo and .pyd files.
# __pycache__/   # Exclude the whole folder


def _add_assets_to_sdist(cpd_conn, asset_list, my_dir, build_dir):
    """Download assets to the sdist directory
    and return corresponding list of lines for MANIFEST.in"""
    logging.info(
        f"_add_assets_to_sdist: {cpd_conn}, {asset_list}, {my_dir}, {build_dir}"
    )
    from pathlib import Path

    #todo: use _download_assets() and map returned paths to manifest
    assert cpd_conn  # future: allow empty dict
    assert isinstance(cpd_conn, dict)
    assert isinstance(asset_list, list)  # may be empty
    assert isinstance(my_dir, Path)
    assert isinstance(build_dir, Path)
    manifest_lines = []
    for asset_ref in asset_list:
        assert isinstance(asset_ref, dict)  # with keys name, type, path
        asset_type = asset_ref.get("asset_type", asset_ref.get("type"))
        assert asset_type  # todo, proper error msg
        name = asset_ref.get("name")
        assert name  # todo, proper error msg
        asset = cpd_lookup_asset(cpd_conn, asset_type, name, version="latest")
        if not asset:
            raise Exception(f"Unable to find {asset_type} '{name}'")
        # print("Error in sdist swenv: asset",name,"not found. Skipping entry.")
        path = asset_ref.get("path", name + ".py")  # todo: name.py if script
        _my_print_msg(f"adding {asset_type} {path}")
        to_path = build_dir / "cpd_software_env" / path
        cpd_download_asset_to_file(cpd_conn, asset=asset, to_path=str(to_path))
        manifest_lines.append(f"include cpd_software_env/{path}")
    return manifest_lines


def _download_assets(cpd_conn,asset_list,target_dir):
    """Download assets to the target directory
    and return corresponding list of file paths relative to target_dir"""
    assert cpd_conn  # future: allow empty dict
    assert isinstance(cpd_conn, dict)
    assert isinstance(asset_list, list)  # may be empty
    assert isinstance(target_dir, Path)
    file_paths = []
    for asset_ref in asset_list:
        assert isinstance(asset_ref, dict)  # with keys name, type, path
        asset_type = asset_ref.get("asset_type", asset_ref.get("type"))
        assert asset_type  # todo, proper error msg
        name = asset_ref.get("name")
        assert name  # todo, proper error msg
        asset = cpd_lookup_asset(cpd_conn, asset_type, name, version="latest")
        if not asset:
            raise Exception(f"Unable to find {asset_type} '{name}'")
        # print("Error in swenv: asset",name,"not found. Skipping entry.")
        path = asset_ref.get("path", name + ".py")  # todo: name.py if script
        _my_print_msg(f"adding {asset_type} {path}")
        cpd_download_asset_to_file(cpd_conn, asset=asset, to_path=str(target_dir / path))
        file_paths.append(path)
    return file_paths
    




def _add_pip_to_sdist(cpd_conn, pip_list, my_dir, build_dir):
    """Download pip packages to the sdist directory
    create a file-based package repository
    and return corresponding list of lines for MANIFEST.in"""
    # print("_add_pip_to_sdist ", cpd_conn, pip_list, my_dir, build_dir)
    import piprepo

    # alternative https://github.com/uranusjr/simpleindex is more complex
    from pathlib import Path

    assert cpd_conn  # future: allow empty dict
    assert isinstance(cpd_conn, dict)
    assert isinstance(pip_list, list)  # may be empty
    assert isinstance(my_dir, Path)
    assert isinstance(build_dir, Path)
    pip_target_dir = build_dir / "cpd_software_env" / "cpd_software_env_pipindex"
    _my_run(f"mkdir -p {pip_target_dir}")
    reqfile_path = _map_pip_packages_list_to_reqfile(pip_list)
    pip_cmd = ["pip", "download", "-r", str(reqfile_path), "-d", str(pip_target_dir)]
    _run_pip(pip_cmd)

    from argparse import Namespace
    from piprepo import command

    args = Namespace(directory=str(pip_target_dir))
    command.build(args)  # creates a subdirectory (pip_target_dir / "simple")

    return [f"recursive-include cpd_software_env/cpd_software_env_pipindex *"]


def _my_run(cmdstr):
    import subprocess

    logging.info(cmdstr)
    run_pip = subprocess.run(
        cmdstr,
        shell=True,
        check=False,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        universal_newlines=True,
    )
    logging.debug(run_pip.stdout)


def _my_write_text(filename, list_of_lines):
    with open(filename, "w") as f:
        f.writelines([line + "\n" for line in list_of_lines])


def _create_modules_sdist(modules):
    """Create a source distribution for a list of plain Python modules.
    Arguments:
    * modules is a list of paths, each path string identifying a Python file"""
    # https://docs.python.org/3/distutils/examples.html
    # from pathlib import Path
    import shutil

    assert isinstance(modules, list)
    if not modules:
        return None

    module_names = [Path(mod).stem for mod in modules]
    sdist_name = "_".join(module_names)

    # temporary directory for building the sdist package
    build_dir = f"/tmp/MyDistModPkg/{sdist_name}"
    shutil.rmtree(f"/tmp/MyDistModPkg/{sdist_name}", ignore_errors=True)
    _my_run(f"mkdir -p {build_dir}")

    _my_write_text(
        f"{build_dir}/setup.py",
        [
            "from distutils.core import setup",
            f"setup(name='{sdist_name}',",
            f"      version = '0.1',",
            f"      py_modules={module_names} )",
        ],
    )

    my_dir = Path(__file__).parent
    for mod in modules:
        assert isinstance(mod, str)
        local_path = my_dir / Path(mod)
        assert local_path.is_file()
        _my_run(f"ln -s {local_path.absolute()} {build_dir}/")

    _my_run(f"(cd {build_dir} && rm -rf ./dist && python setup.py sdist --formats=zip)")

    sdist_file = f"{build_dir}/dist/{sdist_name}-0.1.zip"
    logging.info(f"created sdist {sdist_file}")
    return sdist_file


"""
setup.py for sdist by module is like
```
from distutils.core import setup
setup(name='foobar',
      version='1.0',
      py_modules=['foo', 'bar'],
      )
```

"""



def data_ref_to_data_frame(data_ref):
    """Experimental: read a new (pandas) data frame from a data reference.
    Supports plain csv files as data assets."""
    import pandas as pd
    cpd_conn = {}
    asset_type = data_ref.get("type","data_asset")
    name = data_ref["location"]["name"]
    asset = cpd_lookup_asset(cpd_conn, asset_type, name, version="latest")
    if not asset:
            raise Exception(f"data_ref_to_data_frame: Unable to find {asset_type} '{name}'")
    path = name
    cpd_download_asset_to_file(cpd_conn, asset=asset, to_path=path)
    return pd.read_csv(path) 

# custom database drivers: /user-home/_global_/dbdrivers 
#/opt/ibm/dsdriver/java/db2jcc4.jar
#/opt/jdbc/db2jcc4.jar
#/user-home/_global_/dbdrivers/jdbc/default/db2jcc4.jar
#
# read into df Db2/Db2_ibm_db.ipynb
#
# cpdu.cpd_lookup_asset(cpd_conn,'data_asset',name="db2table1")
# 'attachments': [{ 'asset_type': 'data_asset',
#   'name': 'MYTABLE1',
#   'description': 'remote',
#   'connection_id': 'e608ca20-2076-4248-a518-2d96afcc2307',
#   'connection_path': '/ZQT07793/MYTABLE1',
#   'datasource_type': '506039fb-802f-4ef2-a2bf-c1682e9c8aa2',
#
# util.get_connection_credentials
#     href_conn = '/v2/connections/'+conn_id    
#    /v2/connections/... has non-encrypted password
#    meta_credentials = rest_request_json(client,"GET",href_conn)
#    credentials = meta_credentials['entity']['properties']




# copy of create_software_specification from wml utilities

def _create_software_specification(
    wml_client, base_spec_name, custom_files, custom_name=None, version=None
):
    """
    base_spec_name could be "default_py3.7"
    custom_files is a list of names of the file containing the custom conda definition in YAML notation
    or a custom distribution package as .zip file
    """
    from datetime import datetime

    def _get_data_path(custom_file):
        import os

        file_path = None
        try:
            f_local = open(custom_file, "rb")
            file_path = custom_file
            f_local.close()
        except:
            try:
                asset_path = os.path.join("/project_data/data_asset", custom_file)
                f_asset = open(asset_path, "rb")
                file_path = asset_path
                f_asset.close()
            except:
                pass
        return file_path

    # 0. Get id of base sw spec
    base_id = wml_client.software_specifications.get_uid_by_name(base_spec_name)
    assert base_id is not None
    assert base_id != "Not Found"
    assert isinstance(custom_files, list)

    # 1. Create new software spec
    import os.path

    fname = os.path.basename(custom_files[0])
    if custom_name:
        new_spec_name = custom_name
    else:
        new_spec_name = base_spec_name + " + " + fname

    print(f"swspec '{new_spec_name}' = {base_spec_name} + {fname}")

    while version == "replace":
        id = wml_client.software_specifications.get_uid_by_name(
            new_spec_name
        )  # v3.5 get_id_by_name
        if id and id != "Not Found":
            # print('deleting existing sw_spec')
            # delete associated package ext.
            sw = wml_client.software_specifications.get_details(id)
            assert sw["entity"]["software_specification"]["type"] == "derived"
            for pe in sw["entity"]["software_specification"]["package_extensions"]:
                # print('deleting pe ',pe['metadata']['asset_id'])
                wml_client.package_extensions.delete(pe["metadata"]["asset_id"])
            wml_client.software_specifications.delete(id)
        else:
            break

    ss_metadata = {
        wml_client.software_specifications.ConfigurationMetaNames.NAME: new_spec_name,
        wml_client.software_specifications.ConfigurationMetaNames.DESCRIPTION: datetime.now().strftime(
            "%b%d %H:%M"
        )
        + "\ncreated by cpd_software_env",
        wml_client.software_specifications.ConfigurationMetaNames.BASE_SOFTWARE_SPECIFICATION: {
            "guid": base_id
        },
    }
    ss_asset_details = wml_client.software_specifications.store(meta_props=ss_metadata)

    for custom_file in custom_files:
        # 2. Determine package type from extension of custom_file name.
        fname = os.path.basename(custom_file)
        fbase_ext = os.path.splitext(fname)
        assert len(fbase_ext) >= 2
        if fbase_ext[-1] == ".zip":
            pkg_type = "pip_zip"
        elif fbase_ext[-1] in [".yml", ".yaml"]:
            pkg_type = "conda_yml"
        else:
            raise Exception("file name extension must be .yml, .yaml, or .zip")

        # 3. Create package extension
        # find custom_file in local dir or /project_data/data_asset
        # print('pe',custom_file)
        custom_file_path = _get_data_path(custom_file)
        assert custom_file_path  # tbd error handling

        pe_metadata = {
            wml_client.package_extensions.ConfigurationMetaNames.NAME: fname,
            # wml_client.software_specifications.ConfigurationMetaNames.DESCRIPTION: '...', # optional
            wml_client.package_extensions.ConfigurationMetaNames.TYPE: pkg_type,
        }
        pe_asset_details = wml_client.package_extensions.store(
            meta_props=pe_metadata, file_path=custom_file_path
        )
        pe_asset_id = wml_client.package_extensions.get_uid(pe_asset_details)
        # check with wml_client.package_extensions.list() and/or wml_client.package_extensions.get_uid_by_name(fname)

        # 4. Add package extension to the new software spec
        ss_asset_id = wml_client.software_specifications.get_uid(ss_asset_details)
        wml_client.software_specifications.add_package_extension(
            ss_asset_id, pe_asset_id
        )

    return ss_asset_id

    # optionally check results with
    #  wml_client.software_specifications.list()
    #  wml_client.package_extensions.list()


#


##############################


#################################################################################
#
# Functions copied from other utility modules to make this module self-contained
#
#


######## from wml utilities


def _wml_connect_env(url=None, token=None, version=None, project_id=None, space_id=None):
    """Create WML Python client object.
    Use environment variables to derive credential
    Use API probing to determine the version of the WML service.
    Works for CP4D versions 2.5, 3.0, and 3.5
    """
    import os
    import requests, urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    if not url:
        url = os.getenv("RUNTIME_ENV_APSX_URL")
    if not url:
        url = "https://internal-nginx-svc:12443"  # standard CP4D internal URL
    if not token:
        token = os.environ["USER_ACCESS_TOKEN"]

    wml_credentials = {
        "url": url,
        "token": token,
        "instance_id": "openshift",
        # "version": "2.5.0", 3.0.0", "3.5", or "4.0"  set below
    }

    # Check if the CP4D server has new Spaces API /v2/spaces and WML API /ml/v4/ as in CP4D v3.5 (and WML Cloud)
    # CP4D 3.5 has /v2/spaces while  CP4D 3.0 uses /v4/spaces
    # We check /v2/spaces instead of a true WML method such as /ml/v4/models
    # because the spaces method does not require any additional query parameter
    if not version:
        version = os.getenv("CPD_VERSION")
    headers = {"Authorization": "Bearer " + token}
    response = requests.request(
        "GET", url + "/v2/spaces?limit=1", headers=headers, verify=False
    )
    if response.status_code == 200 or version == "3.5" or version == "4.0":  # CP4D v3.5
        # !pip install --upgrade ibm-watson-machine-learning  # recommended
        if version:
            wml_credentials["version"] = version
        else:
            wml_credentials["version"] = "3.5"
        from ibm_watson_machine_learning import APIClient

        # ibm_watson_machine_learning.version() == version of py package
        client = APIClient(wml_credentials)
    else:  #  CP4D v3.0.x or old CP4D v2.5
        # pip install --upgrade watson-machine-learning-client-V4  # optional
        if not version:
            # check if CP4D has software_specifications as in CP4D v3.x
            response = requests.request(
                "GET",
                url + "/v2/software_specifications",
                headers=headers,
                verify=False,
            )
            if response.status_code == 200:
                version = "3.0.0"
            else:
                version = "2.5.0"
        wml_credentials["version"] = version if version else "3.0.0"
        from watson_machine_learning_client import WatsonMachineLearningAPIClient

        client = WatsonMachineLearningAPIClient(wml_credentials)

    if True:  # verbose
        import sys

        print("WML service in CP4D " + wml_credentials["version"])
        print("WML client lib " + client.version + " in Python " + sys.version[:3])
        # print(module.__file__) or inspect.getfile(...)
    if project_id:
        client.set.default_project(project_id)
    if space_id:
        client.set.default_space(space_id)
    return client


def _save_python_function(client, outer_function, name, sw_spec_name=None, version=None):
    """
    CP4D 3.0 default "ai-function_0.1-py3.6" is plain Python 3.6 with no additional packages.
    Alternative is "default_py3.6" which has many additional packages.
    For CP4D 3.5 with Python 3.7 use sw_spec_name="default_py3.7".
    https://www.ibm.com/support/knowledgecenter/SSQNUZ_3.5.0/wsj/wmls/wmls-deploy-python-types.html
    """
    import sys

    if not sw_spec_name:
        py_version = sys.version[:3]  # 3.6 or 3.7
        if (
            client.__module__ == "watson_machine_learning_client.client"
        ):  # old V4 in CP4D 3.0
            sw_spec_name = "ai-function_0.1-py3.6"
        elif client.wml_credentials["version"].startswith("4."):
            sw_spec_name = "default_py3.7_opence"  # v4
        else:
            sw_spec_name = "default_py" + py_version

    #
    # define meta props
    if client.wml_credentials["version"] == "2.5.0":
        # v2.5 uses runtimes instead of software specs
        # https://www.ibm.com/support/knowledgecenter/en/SSQNUZ_2.5.0/wsj/wmls/wmls-deploy-python.html
        meta_props = {
            client.repository.FunctionMetaNames.NAME: name,
            client.repository.FunctionMetaNames.RUNTIME_UID: sw_spec_name,
        }
    else:
        sw_spec_id = client.software_specifications.get_uid_by_name(sw_spec_name)
        # print("sw_spec",sw_spec_name,sw_spec_id)
        assert sw_spec_id and sw_spec_id != "Not Found"
        meta_props = {
            client.repository.FunctionMetaNames.NAME: name,
            client.repository.FunctionMetaNames.SOFTWARE_SPEC_UID: sw_spec_id,
        }
    #

    if version == "update" or version == "next":
        function_meta = lookup_asset_cpd(client, asset_type="wml_function", name=name)
        function_uid = function_meta["metadata"]["asset_id"]
        if version == "next":
            client.repository.create_function_revision(function_uid)
        metadata = {client.repository.FunctionMetaNames.DESCRIPTION: "updated_function"}
        function_details = client.repository.update_function(
            function_uid, changes=metadata, update_function=outer_function
        )
        return function_uid
    # else:
    if version == "replace":
        _delete_functions(client, name=name)
    # outer_function is the Python function object, not the .py file
    function_artefact = client.repository.store_function(
        meta_props=meta_props, function=outer_function
    )
    function_uid = client.repository.get_function_uid(function_artefact)
    # print("Stored Function UID = " + function_uid)
    return function_uid


def _delete_functions(client, name):
    fns = _cpd_search_assets(_get_cpd_access_info(client), "wml_function", name=name)
    for d in fns:
        print(
            "deleting old function ", d["metadata"]["name"], d["metadata"]["asset_id"]
        )
        assert d["metadata"]["name"] == name
        client.repository.delete(d["metadata"]["asset_id"])
    #


def _get_cpd_access_info(client):
    """Extract access info from WML Python client object"""
    # works with both V4 GA and old V4 beta

    cpd_access_info = (
        client.wml_credentials.copy()
    )  # contains url, token, version, and instance_id
    if not cpd_access_info.get(
        "token"
    ):  # token not set if wml_credentials were defined with uid/passd ?
        cpd_access_info["token"] = client.wml_token
    if cpd_access_info.get("instance_id") == "999":
        cpd_access_info["instance_id"] = "openshift"  # patch issue in old client

    if client.default_project_id:
        cpd_access_info["project_id"] = client.default_project_id
    if client.default_space_id:
        cpd_access_info["space_id"] = client.default_space_id

    return cpd_access_info


def _save_python_script(
    wml_client, name, resource=None, sw_spec_name=None, version="patch"
):
    import sys

    assert name, "_save_python_script:script name missing"
    # derive from resource !?
    assert version == "replace"
    if not resource:
        resource = name
    if version == "replace":
        _delete_scripts(wml_client, name=name)
    #
    if not sw_spec_name:
        sw_spec_name = "default_py" + sys.version[:3] + "_opence"
        sw_spec_id = wml_client.software_specifications.get_uid_by_name(sw_spec_name)
        if not sw_spec_id or sw_spec_id == "Not Found":
            sw_spec_name = "default_py" + sys.version[:3]  # fall back to old py3.x
    #
    sw_spec_id = wml_client.software_specifications.get_uid_by_name(sw_spec_name)
    # print(f"sw_spec {sw_spec_name}  id={sw_spec_id}")
    assert sw_spec_id and sw_spec_id != "Not Found"
    meta_props = {
        wml_client.script.ConfigurationMetaNames.NAME: name,
        wml_client.script.ConfigurationMetaNames.SOFTWARE_SPEC_UID: sw_spec_id,  # required
    }
    # create the asset for the script
    script_details = wml_client.script.store(meta_props, file_path=resource)
    script_id = wml_client.script.get_uid(script_details)
    print("created script ", name, script_id)
    return script_details


def _delete_scripts(wml_client, name):
    scripts = _cpd_search_assets(wml_client, asset_type="script", name=name)
    for d in scripts:
        print("deleting old script ", d["metadata"]["name"], d["metadata"]["asset_id"])
        assert d["metadata"]["name"] == name
        wml_client.script.delete(d["metadata"]["asset_id"])
    #


#


###############################################################################
# begin of cpd_utilities35.py


###################################################
# Utility functions for the Data REST API in CP4D


# * cpd_rest_request35(cpd_access_info,method,request,postdata=None)
# * cpd_search_assets(cpd_access_info,name)
# * cpd_lookup_asset(cpd_access_info,name)
# * cpd_get_asset_content(...)
# * cpd_delete_asset(...)


##########
# This sample code is provided "as is", without warranty of any kind.
##########


# This Python file can be imported as in `from cpd_utilities import *`
# or individual functions can be copied, e.g., via `%load -n cpd_conn_default_project`
# to make the main script or notebook self-contained.
# The function names generally have the prefix "cpd_".
# The functions are not designed as classes and methods because that would
# make copy&paste more involved.





#######################################################################################
#
# Utility functions for Data REST API in CP4D
#

# Calls to the REST APIs in CP4D generally need
# * the URL of the CP4D system
# * a bearer token to authenticate the user
# * an of the project or deployment space
# The values are provided by the caller or extracted from environment variables.
# These attributes are passed to the utility function in a simple dictionary.


def _cpd_conn_default_project():
    """Get CP4D connection info for default project."""
    import os

    return _cpd_conn_complete({"project_id": os.environ["PROJECT_ID"]})


def _cpd_conn_default_space():
    """Get CP4D connection info for default space."""
    import os

    return _cpd_conn_complete({"space_id": os.environ["SPACE_ID"]})


def _cpd_conn_complete(access_info):
    """Derive a complete CP4D connection dictionary for calls of the REST API.
    Attributes are derived from access_info argument and from environment variables.
    Input argument may also be a wml client object.
    Returns a dictionary with url, token, version?
    """
    import os
    import copy

    # 1. extract properties from access_info argument

    if isinstance(access_info, dict):
        cpd_conn = copy.deepcopy(access_info)
    else:
        # access_info is assumed to be WML client object
        # e.g. type(wml_client) == ibm_watson_machine_learning.client.APIClient
        cpd_conn = {}
        cred = access_info.wml_credentials
        cpd_conn["url"] = cred.get("url")
        cpd_conn["token"] = cred.get("token")
        # 'instance_id': 'openshift', 'version': '3.5'
        #  wml client has 'default_project_id' or 'default_space_id'
        if access_info.default_project_id:
            cpd_conn["project_id"] = access_info.default_project_id
        if access_info.default_space_id:
            cpd_conn["space_id"] = access_info.default_space_id

    # 2. fill in blanks from environment variables

    if not cpd_conn.get("url"):
        cpd_conn["url"] = os.getenv(
            "RUNTIME_ENV_APSX_URL", "https://internal-nginx-svc:12443"
        )
        # nginx url works when running in the CP4D cluster

    if not cpd_conn.get("token"):
        token = os.getenv("USER_ACCESS_TOKEN", os.getenv("PROJECT_ACCESS_TOKEN"))
        # RStudio (job) runtime may have PROJECT_ACCESS_TOKEN instead of USER_ACCESS_TOKEN
        # Token in RStudio may start with "bearer ..."
        if token and token.lower().startswith("bearer "):
            token = token[7:]
        cpd_conn["token"] = token

    if not cpd_conn.get("space_id") and not cpd_conn.get("project_id"):
        cpd_conn["space_id"] = os.getenv("SPACE_ID")

    if not cpd_conn.get("project_id") and not cpd_conn.get("space_id"):
        cpd_conn["project_id"] = os.getenv("PROJECT_ID")

    cpd_conn["is_complete"] = True
    return cpd_conn


###############
# Submit requests
#
# Sample usage:
#    _cpd_rest_request35({},"GET","/v2/spaces?limit=1")
# or
#    cpd_conn = cpd._cpd_conn_default_project()
#    r = cpd._cpd_rest_request35(cpd_conn,"POST","/v2/asset_types/data_asset/search",json={'query':'*:*'})
#    r.json()

import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def _cpd_rest_request35(
    cpd_access_info,
    method,
    request,
    postdata=None,
    json=None,
    files=None,
    verbose=False,
):
    """Call REST API in CP4D.
    The full URL with query options for the actual REST request is
    constructed from the cpd_access_info and request parameters.
    * cpd_access_info is a dictionary providing the url, location, and access token.
    * method is "GET", "POST", "PUT", PATCH", or "DELETE"
    * request is the path of the particular resource such as "/v2/assets"
    * json or postdata is assumed to be a dictionary that can be serialized to JSON
    Return response of REST request or raise an Exception if the request returned an error status code
    """
    # * gather url, authentication from cpd_conn or environment variables
    # * extend the request by query parameters that are specific to CP4D
    #   such as ?project_id=...&version=...
    #   (if the parameters are not already included in the request)
    # * return exception if response.status_code not in 20x range
    import requests, os
    from urllib.parse import urljoin

    logging.debug(f"cpd_util:rest_request(...,{method},{request},{postdata},{json})")
    # hide connection info with token

    # 1. get the complete connection
    if isinstance(cpd_access_info, dict) and cpd_access_info.get("is_complete"):
        cpd_conn = cpd_access_info
    else:
        cpd_conn = _cpd_conn_complete(cpd_access_info)

    # 2. compose the full http URL incl query parameters
    # url = cpd_conn["url"].rstrip('/')
    # if url.endswith("/zen") : url = url[:-4] # e.g. when url was copied from browser
    # url_request = url+'/'+request.lstrip('/')
    url_request = urljoin(cpd_conn["url"], request)
    params = {"version": cpd_conn.get("version", "2021-06-01")}
    # caller can eliminate version param by passing cpd_access_info={'version':None}
    # add location as query argument if it is not already included in the request
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(request)
    pq = parse_qs(parsed.query)
    if not pq or not (
        pq.get("project_id") or pq.get("space_id") or pq.get("catalog_id")
    ):
        # add project_id or space_id to query, if available in cpd_conn
        params["project_id"] = cpd_conn.get("project_id")
        params["space_id"] = cpd_conn.get("space_id")
        params["catalog_id"] = cpd_conn.get("catalog_id")

    # 3. submit the request to CP4D system and check response
    assert cpd_conn.get("token")
    headers = {
        "Authorization": f'Bearer {cpd_conn["token"]}',
        #'Content-Type': 'application/json'   # not valid with 'files' data
    }
    if postdata or json:
        headers["Content-Type"] = "application/json"
    logging.debug(
        f"cpd_util:rest_request({method},{url_request},params={params},postdata={postdata},json={json})"
    )
    logging.debug(f"cpd_util:rest_request headers {headers}")
    response = requests.request(
        method,
        url_request,
        params=params,
        json=(json if json else postdata),
        headers=headers,
        files=files,
        verify=False,
        timeout=100,
    )
    if os.getenv("CPD_REQUEST_SHOW_CURL"):  # print corresponding curl command
        # e.g. when os.environ["CPD_REQUEST_SHOW_CURL"] = "1"
        import json as jsonpkg  # naming conflict wiith parameter "json"

        print("curl -k -X", method, '-H "Authorization: Bearer $USER_ACCESS_TOKEN" \\')
        for k in headers:
            if k != "Authorization":
                print(f"    -H '{k}: {headers[k]}' \\")
        if postdata or json:
            print(f"    -d '{jsonpkg.dumps(json if json else postdata)}' \\")
        if files:
            print("    -F file=@path/to/your/file \\")
        print(f"    '{response.url}'")
    logging.debug(f"cpd_util:rest_request returned status {response.status_code}")
    logging.debug(f"cpd_util:rest_request response.text : {response.text[:50]}...")
    if response.status_code not in [200, 201, 202, 204]:
        print("Request failed :", response.status_code)
        print(response.url)
        raise Exception(f'REST returned code {response.status_code} "{response.text}"')
    return response


# DELETE method might return status 204
# HTTP Status 204 (No Content) indicates that the server has successfully fulfilled the request
# and that there is no content to send in the response payload body.
#

# troubleshoot:
#
# If URL does not start with http: or https:
# request raises exception
# requests.exceptions.MissingSchema: Invalid URL 'bla/v2/jobs': No schema supplied. Perhaps you meant http://bla/v2/jobs?
#
# Exception: REST returned code 504 "<html>
# <head><title>504 Gateway Time-out</title></head>
# Cause: token is invalid
# Both in CP4D v3.0 and v3.5
#
#     raise Exception(f'REST returned code {response.status_code} "{response.text}"')
# Exception: REST returned code 400 "{"code":400,"error":"Bad Request","reason":"Invalid resource guid format.","message":"The server cannot or will not process the request due to an apparent client error (e.g. malformed request syntax)."}"
# Cause: could be invalud project id parameter in URL
#
# Exception: REST returned code 400 "{"code":400,"error":"Bad Request","reason":"Missing or Invalid Data","message":"invalid signature"}"
# bad token
#
# HTTPIO_ERROR_SEND_STATE sap





#


##########  Assets and asset types


def _cpd_get_asset_types(cpd_access_info):
    res = _cpd_rest_request35(cpd_access_info, "GET", "/v2/asset_types")
    l = res.json()["resources"]
    return [(t["name"], t.get("description")) for t in l]


# How to get all asset types:
# cpd_access_info = {"project_id":os.getenv("PROJECT_ID")} # connect to current project
# tl = cpd_rest_request_json(cpd_access_info,"GET","/v2/asset_types")
# [ t.get("name")  for t in tl.get("resources")]


# Generic asset search function using CP4D Data REST API
# "/v2/asset_types/"+asset_type+"/search"
# usage e.g.:
#    _cpd_search_assets(cpd_access_info,"notebook",name="HelloWorld",sortby="created_at")
#
def _cpd_search_assets(
    cpd_access_info, asset_type, name=None, query=None, sortby=None, verbose=False
):
    """Search assets based on name or query pattern.
    Provide either name or query as argument.
    * cpd_access_info: see cpd_rest_request(cpd_access_info,...)
    * asset_type can be, e.g., "data_asset", "script", "wml_model", ...
    * query can be "*:*" to match any asset
      or complex such as "job_run.job_ref:<job_id> OR job_run.job_ref:<job_id> OR ..."
    * sortby can be "last_updated_at" or "created_at"
    """
    # Lucene syntax https://lucene.apache.org/core/2_9_4/queryparsersyntax.html#Wildcard%20Searches

    #
    # Check parameters
    logging.debug(f"search_assets({asset_type},{name},{query})")
    assert asset_type

    if name:
        pattern = name.replace(" ", "\\ ").replace("/", "\\/").replace(":", "\\:")
        # '/' is a special character in CAMS search function, needs to be escaped
        postquery = {"query": "asset.name:" + pattern}
    else:
        postquery = {"query": (query if query else "*:*")}

    response = _cpd_rest_request35(
        cpd_access_info,
        "POST",
        f"/v2/asset_types/{asset_type}/search",
        postdata=postquery,
    )
    # empty search result would have .status_code 200 and .text {"total_rows":0,"results":[]}

    l = response.json()["results"]
    if not l or not sortby:
        return l
    elif sortby == "last_updated_at" or sortby == "modified_at":
        if l[0]["metadata"].get("usage"):  # as in cpd 3.5:
            return sorted(l, key=lambda d: d["metadata"]["usage"]["last_updated_at"])
        else:  # cpd 3.0
            return sorted(l, key=lambda d: d["metadata"]["modified_at"])
        # in cpd 3.5: d["metadata"] 'usage': {'last_updated_at': ...
    elif sortby == "created_at":
        return sorted(l, key=lambda d: d["metadata"]["created_at"])
    # else
    raise exception("sortby '{}' not supported".format(sortby))


#

# Notice the results of an Asset Type Search, as shown above, only contain the "metadata" section
# of a primary metadata document. In particular, the "entity" section that contains the attributes
# is not returned. That is done to reduce the size of the response because, in general,
# the "entity" section of a primary metadata document can be much larger than the "metadata" section.
# Use the value of the "metadata.asset_id" in one of the items in "results" to retrieve more details.
# https://cloud.ibm.com/apidocs/watson-data-api-cpd#search-asset-type-attribute-boo


def cpd_lookup_asset(
    cpd_access_info, asset_type, name=None, id=None, href=None, version=None
):
    """Lookup a asset by name.
    * cpd_access_info: see cpd_rest_request(cpd_access_info,...)
    Returns metadata for asset or None if not found.
    Can raise an Exception when lookup by name finds duplicates.
    """
    # also used in deployments when resolving data references (in batch jobs)

    if href:
        return _cpd_rest_request35(cpd_access_info, "GET", href).json()

    if id:
        assert isinstance(id, str)
        return _cpd_rest_request35(cpd_access_info, "GET", "/v2/assets/" + id).json()
        # similar to client.data_assets.get_details(id)

    # else lookup by name
    sortby = "last_updated_at" if version == "latest" else None
    l = _cpd_search_assets(cpd_access_info, asset_type, name=name, sortby=sortby)
    if not l:
        return None
    meta = None
    if len(l) == 1:
        meta = l[0]
    else:
        assert len(l) >= 2
        if version == "latest":
            meta = l[-1]  # last / most recent item
        else:
            raise Exception("Asset name not unique : " + name)
    assert meta is not None
    return _cpd_rest_request35(cpd_access_info, "GET", meta["href"]).json()


#######   download
# Get asset content (download)

# Data API https://cloud.ibm.com/apidocs/watson-data-api-cpd#introduction

# "Get a data asset" https://cloud.ibm.com/apidocs/watson-data-api-cpd#getdataassetv2

# sample curl commands https://github.ibm.com/PrivateCloud-analytics/Zen/issues/22000
# incl downloading files


def cpd_download_asset_to_file(
    cpd_access_info, asset=None, id=None, href=None, to_path=None
):
    """Download asset content into a local file
    * asset, id, or href must be defined
    """
    # also used in deployments when resolving data references
    from pathlib import Path

    if not asset:
        if not href:
            assert id
            href = "/v2/assets/" + id
        asset = _cpd_rest_request35(cpd_access_info, "GET", href).json()
    assert isinstance(asset, dict)  # metadata dictionary

    fpath = Path(to_path if to_path else meta["metadata"]["name"])
    fpath.parent.mkdir(parents=True, exist_ok=True)

    res = _cpd_get_asset_content(cpd_access_info, asset=asset)
    with open(str(fpath), "wb") as f:
        f.write(res.content)


def _cpd_get_asset_content(cpd_access_info, asset=None, id=None, href=None):
    """Get the content of an asset such as a notebook or data asset.
    * asset, id, or href must be defined
    Returns response object
    Caller can access data using r.content or r.text,r.encoding
    Restriction: only first attachment
    """
    if not asset:
        if not href:
            assert id
            href = "/v2/assets/" + id
        asset = _cpd_rest_request35(cpd_access_info, "GET", href).json()
    # Get attachment
    asset_id = asset["metadata"]["asset_id"]
    attachment_id = asset["attachments"][0]["id"]
    attachment_details = _cpd_rest_request35(
        cpd_access_info, "GET", f"/v2/assets/{asset_id}/attachments/{attachment_id}"
    )
    return _cpd_rest_request35(cpd_access_info, "GET", attachment_details.json()["url"])


# Sample usage:
# cpd_access_info = {"project_id":os.getenv("PROJECT_ID")}
# assets = cpd_rest_search_assets(cpd_access_info,"notebook",name="HelloWorld",sortby="created_at")
# response = _cpd_get_asset_content(cpd_access_info,href=assets[-1]["href"])
# with open("tmpfile","wb") as f:
#     f.write(response.content)





##################
#
# Delete asset


def _cpd_delete_asset(cpd_access_info, asset_type, id, force_read_only=False):
    """Delete a 'regular' asset such as notebook, script.
    Uses "DELETE /v2/assets/{id}?purge_on_delete=true"
    Use other specific Delete requests for objects such as Connections.
    """
    #
    logging.info(f"Deleting asset {asset_type} {id}")
    res = _cpd_rest_request35(cpd_access_info, "GET", f"/v2/assets/{id}")
    asset = res.json()
    if not asset:
        return None

    if asset_type == "connection":
        print("_cpd_delete_asset connection")
        _cpd_rest_request35(cpd_access_info, "DELETE", f"/v2/connections/{id}")
        return None
    # Regular "DELETE",f"/v2/assets/{id}?purge_on_delete=true"
    # on connection with personal credentials returns:
    # Exception: REST returned code 400 "{"trace":"el7urqcytkidt4aczfwb30jdv","errors":
    # [{"code":"ReservedValue","message":"Background delete processing has not finished yet
    # for 40a0b326-feb2-43ff-8585-4cca5f0045e4 / c41c137c-cd56-4685-9cb4-5f3afeb527f0"}]}"

    res = _cpd_rest_request35(
        cpd_access_info, "DELETE", f"/v2/assets/{id}?purge_on_delete=true"
    )  # ,verbose=True)
    # A regular DELETE will just move the metadata to the trash bin (and could be restored)
    # With purge_on_delete the metadata entry is gone.
    # The option purge_on_delete will also delete the attached files
    # unless the attachment has a flag object_key_is_read_only == True.
    # Assets are usually created with object_key_is_read_only == False.

    if not force_read_only:
        return
    # Proceed if you want to delete "read_only" attachments.
    # Note:
    # In the POST input the attribute is called "object_key_is_read_only"
    # In the resulting entity from GET it is called "is_object_key_read_only"
    for att in asset.get("attachments", []):
        if not att.get("is_object_key_read_only"):
            continue  # already purged
        print(
            "Deleting attachment",
            att["is_user_provided_path_key"],
            att.get("object_key"),
        )
        for k in [
            "is_remote",
            "is_managed",
            "is_referenced",
            "is_object_key_read_only",
        ]:
            print(k, "=", att.get(k), end=" ")
        print("")  # att.get()'object_key')
        if att["is_user_provided_path_key"] and att.get("object_key"):
            # DELETE /v2/asset_files/{path}
            att_path = att["object_key"].lstrip("/")
            try:
                _cpd_rest_request35(
                    cpd_access_info, "DELETE", f"/v2/asset_files/{att_path}"
                )  # ,verbose=True)
                print("    ... success")
            except Exception as ex:
                print("    ... gone")


def _cpd_delete_assets(cpd_access_info, asset_type, name):
    assets = _cpd_search_assets(cpd_access_info, asset_type, name=name)
    for asset in assets:
        _cpd_delete_asset(cpd_access_info, asset_type, id=asset["metadata"]["asset_id"])


# end of cpd_utilities35.py
#####################################


#######################################
#
# cpd_access.py
#
# lookup_space


def _lookup_cpd_space(url, token, name):
    """Get information about a space in CP4D.
    * name is the name of the deployment space to look up.
    Uses /v2/spaces which is available in CP4D 3.5 but not in v3.0
    """
    import requests

    # from posixpath import join as urljoin
    from urllib.parse import urljoin

    header = {"Content-Type": "application/json", "Authorization": "Bearer " + token}
    response = requests.get(
        urljoin(url, "/v2/spaces"), headers=header, verify=False, timeout=100
    )
    if response.status_code == 404:  # might be old cpd
        return None
    if response.status_code != 200 and response.status_code != 201:
        raise Exception(response.text)
    #
    prjl = [
        prj for prj in response.json()["resources"] if prj["entity"]["name"] == name
    ]
    return prjl[0] if prjl else None


def _lookup_cpd_space_id(url, token, name):
    space = _lookup_cpd_space(url, token, name)
    return space["metadata"]["id"] if space else None
