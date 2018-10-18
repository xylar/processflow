
source $MODULESHOME/init/bash
if [ ${HOSTNAME:0:3} == "nid" ]; then
  module unload python
  module unload python_base
  source /global/project/projectdirs/acme/software/anaconda_envs/edison/base/etc/profile.d/conda.sh
  conda activate e3sm_unified_1.2.3_py2.7_nox
  export NCO_PATH_OVERRIDE=No
elif [ ${HOSTNAME:0:4} == "cori" ]; then
  module unload python
  module unload python_base
  source /global/project/projectdirs/acme/software/anaconda_envs/cori/base/etc/profile.d/conda.sh
  conda activate e3sm_unified_1.2.3_py2.7_nox
  export NCO_PATH_OVERRIDE=No
elif [ ${HOSTNAME:0:4} == "rhea" ] || [ ${HOSTNAME:0:5} == "titan" ]; then
  module unload python
  source /ccs/proj/cli900/sw/rhea/e3sm-unified/base/etc/profile.d/conda.sh
  conda activate e3sm_unified_1.2.3_py2.7_nox
  export NCO_PATH_OVERRIDE=No
elif [ ${HOSTNAME:0:5} == "aims4" ] || [ ${HOSTNAME:0:5} == "acme1" ]; then
  source /usr/local/e3sm_unified/envs/base/etc/profile.d/conda.sh
  conda activate e3sm_unified_1.2.3_py2.7_nox
  export NCO_PATH_OVERRIDE=No
elif [ ${HOSTNAME:0:4} == "wolf" ] || [ ${HOSTNAME:0:7} == "grizzly" ]; then
  module unload python
  source /usr/projects/climate/SHARED_CLIMATE/anaconda_envs/base/etc/profile.d/conda.sh
  conda activate e3sm_unified_1.2.3_py2.7_nox
elif [ ${HOSTNAME:0:6} == "blogin" ] || ([ ${HOSTNAME:0:1} == "b" ] && [[ ${HOSTNAME:1:2} =~ [0-9] ]]); then
  source /lcrc/soft/climate/e3sm-unified/base/etc/profile.d/conda.sh
  conda activate e3sm_unified_1.2.3_py2.7_nox
  unset LD_LIBRARY_PATH
elif [ ${HOSTNAME:0:6} == "cooley" ] || ([ ${HOSTNAME:0:2} == "cc" ] && [[ ${HOSTNAME:2:3} =~ [0-9] ]]); then
  source /lus/theta-fs0/projects/ClimateEnergy_2/software/e3sm_unified/base/etc/profile.d/conda.sh
  conda activate e3sm_unified_1.2.3_py2.7_nox
else
  echo "Running on an unsupported machine. Sourcing users anaconda environment from {{ user_env_path }}"
  source activate {{ user_env_path }}
fi

{{ cmd }}
