# coding: utf-8
import abc
#from pyspark import SparkConf
#from pyspark.sql import SparkSession
import logging.handlers
import sys
import os


class InitSpark():
    """
    해당 인터페이스를 사용할 경우
    global로 spark과 LOG를 사용할수 있는 환경이 구성됨

    spark   spark을 이용한기 위한 context
    LOG     LOG를 출력하기 위한 모듈, python 기본 logger모듈을 활용하기 때문에 아래와 같은 로그 레벨이 존재
            LOG.debug({msg})
            LOG.info({msg})
            LOG.warn({msg})
            LOG.error({msg})
            LOG.critical({msg})
    """
    __metaclass__ = abc.ABCMeta
    SPARK_MASTER = 'spark://192.168.100.180:7077'

    LOG_FORMAT = '[%(asctime)s] %(process)s-%(thread)s, %(levelname)s, %(filename)s, %(lineno)d, %(funcName)s, %(msg)s'
    LOG_TIME_FORMAT = '%Y/%m/%d %H:%M:%S'
    LOG_MAX_BYTE = 10000000
    LOG_BACKUP_COUNT = 10

    JAR_DRIVER_PATH = '/root/user/anhm/Spark-on-IRIS/lib/java'
    JAR_EXECUTOR_PATH = '/mobigen/tools/Spark-on-IRIS/lib/java/'
    JAR_NAME = [
        'mobigen-iris-jdbc-2.1.0.4.jar',
        'iris-spark-datasource-1.6-spark-2.4.4_2.11-bin.jar'
    ]

    def __init__(self, app_name, log_path=None, spark_master=None):
        """
        :param app_name:        spark이 실핼될때 사용되는 application name
                                    ex) TestSparkProgram
        :param log_path:        로그의 저장 경로
                                None: 로그를 화면에 출력
                                str : 로그가 저장될 경로, 파일은 BaseRotatingHandler 이 이용됨
                                    ex) /tmp/test.log
        :param spark_master:    spark 마스터 주소
                                None: 자체적인 spark을 구성하여 실행
                                str : 임의의 spark 클러스터로 접속
                                    ex) Nonespark://master:7077
        """
        self._app_name = app_name
        self._log_path = log_path
        self._spark_conf = SparkConf()

        if spark_master is None:
            self._spark_master = IRISSpark.SPARK_MASTER
        else:
            self._spark_master = spark_master

        if log_path is None:
            self._init_stdout_logger()
        else:
            self._init_file_logger()

        self._spark_conf.set('spark.app.name', self._app_name)
        self._spark_conf.set('spark.master', self._spark_master)
        self._spark_conf.set('spark.cores.max', '1')
        self._spark_conf.set('spark.executor.memory', '1g')

        self._spark_conf.set('spark.driver.extraClassPath',
            ':'.join(map(lambda c: "{}/{}".format(IRISSpark.JAR_DRIVER_PATH, c), IRISSpark.JAR_NAME))
        )
        self._spark_conf.set('spark.executor.extraClassPath',
            ':'.join(map(lambda c: "{}/{}".format(IRISSpark.JAR_EXECUTOR_PATH, c), IRISSpark.JAR_NAME))
        )

    def set_config(self, key, value):
        """
        사용자가 spark config를 직접 설정할 경우에 사용
        다음 사이트를 참고: https://spark.apache.org/docs/2.4.0/configuration.html

        :param key:     config의 key에 해당하는 값
                        ex) spark.cores.max
        :param value:   config의 value에 해당하는 값
                        ex) 1
        """
        self._spark_conf.set(key, value)

    def init_resource(self, cores_max=None, executor_cores=None, executor_memory=None, driver_cores=None, driver_memory=None):
        """
        resource 설정으로 많이 사용되는 config 값을 손쉽게 설정할수 있도록 제공

        :param cores_max:           spark.cores.max
        :param executor_cores:      spark.executor.cores
        :param executor_memory:     spark.executor.memory
        :param driver_cores:        spark.driver.cores
        :param driver_memory:       spark.driver.memory
        """
        if cores_max is not None:
            self._spark_conf.set('spark.cores.max', core_max)

        if executor_cores is not None:
            self._spark_conf.set('spark.executor.cores', executor_cores)

        if executor_memory is not None:
            self._spark_conf.set('spark.executor.memory', executor_memory)

        if driver_cores is not None:
            self._spark_conf.set('spark.driver.cores', driver_cores)

        if driver_memory is not None:
            self._spark_conf.set('spark.driver.memory', driver_memory)

    def init_port(self, ui_port=None, blockmanager_port=None, driver_blockmanager_port=None):
        """
        구동시 필요한 port정보를 손쉽게 설정할수 있도록 제공
        """
        if ui_port is not None:
            self._spark_conf.set('spark.ui.port', ui_port)

        if blockmanager_port is not None:
            self._spark_conf.set('spark.blockManager.port', blockmanager_port)

        if driver_blockmanager_port is not None:
            self._spark_conf.set('spark.driver.blockManager.port', driver_blockmanager_port)

        # self._spark_conf.set('spark.driver.host', '')
        # self._spark_conf.set('spark.driver.bindAddress', '')

    def init(self, conf=None):
        """
        spark에 resource를 할당받기 위한 작업을 진행

        :param conf:    사용자가 원하는 config값이 정의된 config
                        from pyspark import SparkConf
                        로 설정된 config값을 사용하거나,
                        get_conf 메소드로 전달받은 내용을 수정하여 사용함
        """
        global spark

        if conf is not None:
            self._spark_conf = conf
        else:
            conf = self._spark_conf

        spark = SparkSession.builder\
            .config(conf=self._spark_conf)\
            .getOrCreate()

    def get_conf(self):
        """
        현재 설정되어 있는 config를 전달받는 용도
        """
        return self._spark_conf

    def _init_stdout_logger(self):
        global LOG
        LOG = logging.getLogger(self._app_name)

        stream_handler = logging.StreamHandler(sys.stdout)
        _formatter = logging.Formatter(IRISSpark.LOG_FORMAT, IRISSpark.LOG_TIME_FORMAT)
        stream_handler.setFormatter(_formatter)
        LOG.addHandler(stream_handler)
        LOG.setLevel(logging.INFO)

    def _init_file_logger(self):
        global LOG

        LOG = logging.getLogger(self._app_name)
        log_file_path = os.path.join(self._log_path)

        if not os.path.exists(os.path.dirname(log_file_path)):
            os.makedirs(os.path.dirname(log_file_path))

        file_handler = logging.handlers.RotatingFileHandler(
                log_file_path,
                maxBytes=IRISSpark.LOG_MAX_BYTE,
                backupCount=IRISSpark.LOG_BACKUP_COUNT
            )

        _formatter = logging.Formatter(IRISSpark.LOG_FORMAT, IRISSpark.LOG_TIME_FORMAT)
        file_handler.setFormatter(_formatter)
        LOG.addHandler(file_handler)
        LOG.setLevel(logging.INFO)

    def set_log_level(self, level):
        """
        출력 로그의 레벨을 변경하기 위해 사용

        :param level:   로그의 레벨을 설정함, 로그의 레벨은 아래와 각 레벨에 맞는 숫자를 입력해 주거나
                            set_log_level(30)

                            각 숫자에 해당하는 레벨
                                NOTSET = 0
                                DEBUG = 10
                                INFO = 20
                                WARN = 30
                                ERROR = 40
                                FATAL = 50
                                CRITICAL = 50

                        아래와 같이 logger를 import하여 그해당하는 값을 사용할수 있음
                            import logger
                            set_log_level(logger.WARN)

                            사용 가능한 레벨
                                logger.NOTSET
                                logger.DEBUG
                                logger.INFO
                                logger.WARN
                                logger.ERROR
                                logger.FATAL
                                logger.CRITICAL
        """
        LOG.setLevel(level)

    @abc.abstractmethod
    def process(self):
        pass


class Test(InitSpark):
    def process1(self):
        irisDF = spark.read.format("iris")\
            .option("host", "127.0.0.1")\
            .option("version", "v2")\
            .option("user", "user" )\
            .option("passwd", "password")\
            .option("table", "EVA.SYSLOG")\
            .option("hint", "LOCATION(PARTITION > '20200901900000')")\
            .load()
        irisDF.registerTempTable("angora")
        spark.sql("select DATETIME, HOST from angora where (HOST='gcs0' or HOST!='gcs3') and PROGRAM!='CROND' ").show()

        #import time
        #time.sleep(1000)

if __name__ == '__main__':
    iris_spark = Test('test', './a.log')
    # iris_spark.init()
    # iris_spark.process()

