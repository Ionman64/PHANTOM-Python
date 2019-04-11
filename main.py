#2019-03-31
#ionman64
#PHANTOM Python Port
import sys
import subprocess
import os
import feature_extractor
import csv

def currentFileName():
    return os.path.basename(__file__)

def generateGitLog(input_dir, output_file=None):
    proc = subprocess.run(["git", "--git-dir", "".join([input_dir, os.path.sep, ".git"]), "log", '--format="%H","%P","%an","%ae","%at","%cn","%ce","%ct"'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    if proc.stderr:
        return couldNotCloneRepo()
    if output_file is None:
        return proc.stdout
    with open(output_file, "wb") as file:
        file.write(proc.stdout)
    return operationComplete()

def cloneRepo(url, location):
    subprocess.run(["git", "clone", url, location])

def features(args):
    if len(args) < 3:
        return mustIncludeRepoLocation()
    if (len(args) < 4):
        return mustIncludeOutputLocation()
    if (len(args) < 5):
        return mustIncludeGitLogLocation()
    if (len(args) < 6):
        return mustIncludeTSLocation()

    repoDir = args[2]
    output_git_file = args[3]
    output_file = args[4]
    output_ts_file = args[5]
    if repoDir != "-log_file":
        generateGitLog(repoDir, output_git_file)
    arr = feature_extractor.extract_all_measures_from_file(output_git_file, output_ts_file)

    with open(output_file, "w") as file:
        csv_writer = csv.writer(file, quoting=csv.QUOTE_NONE)
        ready_to_write_headers = ["measure"] + arr["integrations"].keys_order()
        csv_writer.writerow(ready_to_write_headers)
        for measure in sorted(arr.keys()):
            feature_vector = arr[measure]
            feature_vector_values = []
            feature_vector_dict = feature_vector.to_dict()
            for feature in sorted(feature_vector_dict.keys()):
                feature_vector_values.append(feature_vector_dict[feature])
            ready_to_write_list = [measure] + feature_vector_values
            csv_writer.writerow(ready_to_write_list)
    
def clone(args):
    if len(args) < 3:
        return mustIncludeRepoUrl()
    if len(args) < 4:
        return mustIncludeRepoLocation()
    url = sys.argv[2]
    location = sys.argv[3]
    showCloningMessage()
    cloneRepo(url, location)
    return operationComplete()

def help(args=None):
    print ("""
        Usage: %s [args] <repository_location> | <repository_url>

        args:
            --help | -help
                Displays this message
            --analyse | -a 
                Analyses a given repository and produces a CLI result
                e.g. %s --analyse c:\myGitProject
            --features | -f
                Extracts the features from a Git repository and outputs the results to a given file
            --clone | -c 
                Downloads a repository using Git to a given directory 
                e.g. %s --clone https://www.github.com/myGitProject c:\myGitProject
    """ % (currentFileName(), currentFileName(), currentFileName()))

#RESPONSES

def analyse(args):
    print ("Analyse")

def unknownArg(arg = ""):
    print ("Error: unknown arguement %s\n" % arg)

def missingArgs():
    print ("Error: missing arguements\n")

def mustIncludeRepoUrl():
    print ("Error: must include repo url")

def mustIncludeRepoLocation():
    print ("Must include repo directory or log path")

def showCloningMessage():
    print ("Cloning with Git")

def couldNotCloneRepo():
    print ("Could not clone the Repo")

def couldNotExtractFeatures():
    print ("Could not extract features")

def mustIncludeRepoLogPath():
    print ("Must Include Repo Log Path")

def mustIncludeOutputLocation():
    print ("Must include file output location")

def mustIncludeGitLogLocation():
    print ("Must include git log file output location")

def mustIncludeTSLocation():
    print ("Must include time series file output location")

def operationComplete():
    print ("Done!")

#MENU
MENU_OPTIONS = {}
MENU_OPTIONS["--help"] = help
MENU_OPTIONS["-help"] = help
MENU_OPTIONS["--analyse"] = analyse
MENU_OPTIONS["-a"] = analyse
MENU_OPTIONS["--clone"] = clone
MENU_OPTIONS["-c"] = clone
MENU_OPTIONS["--features"] = features
MENU_OPTIONS["-f"] = features

if __name__ == "__main__":
    if (len(sys.argv) < 2):
        missingArgs()
        help()
        exit(0)
    if sys.argv[1] not in MENU_OPTIONS.keys():
        unknownArg(sys.argv[1])
        help()
    else:
        MENU_OPTIONS[sys.argv[1]](sys.argv)
