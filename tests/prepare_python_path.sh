(

  export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
  py4j_path=$( find "${SPARK_HOME}/python/lib/" -name py4j*.zip )
  export PYTHONPATH=${py4j_path}:$PYTHONPATH

  tests_path=$( cd `dirname $0` ; echo `pwd` )
  project_path=$( dirname ${tests_path} )
  export PYTHONPATH=${project_path}:$PYTHONPATH

  echo "export PYTHONPATH=${PYTHONPATH}"
)
