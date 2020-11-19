# IRISSpark 사용 방법

- python언어를 이용하여 Spark에 접속하기 위한 템플릿을 제공
- 해당 라이브러리는 `분석2번 서버` 에서만 사용이 가능하며, 해당 서버의 환경에 맞춰 설정이 되어 있습니다.

## 기본 사용 방법

### 코드 작성 방법

```
import IRISSPark

class Test(IRISSpark.Init):
    def process(self):
        pass
```

1. `IRISSpark` 을 import를 합니다.
   - `python3` 환경에만 설정이 이루어져 있기 때문에 `python2` 에서는 라이브러리를 찾을수 없습니다.
2. 생성하려는 클래스에 `IRISSpark.Init` 을  상속 받습니다.
3. 상속을 받아 작성하는 클래스에서는  `def process(self)` 메소드를 작성해야 하며, 해당 메소드에 사용자가 원하는 프로그램을 적상하면 됩니다.
   - 만약 해당 메소드를 생성하지 않으면 다음과 같은 에러가 발생하게 됩니다.

### 코드 실행 방법

```
iris_spark = Test('TestApp')
iris_spark.init()
iris_spark.process()
```

- `iris_spark = Test('TestApp')`

  - 사용자가 작성한 클래스는 `IRISSpark.Init` 을 상속받아 사용하고 있으며, `IRISSpark.Init`의 생성자인 `__init__` 은 다음과 같은 형태를 가지고 있습니다.

    ```
    def __init__(self, app_name, log_path=None, spark_master=None):
    ```

    - `app_name` spark을 실행할 경우 사용될 app name을 지정 합니다.
    - `log_path` 은 로그를 저장할 경로를 지정합니다. 만약에 해당 인자값이 설정되지 않을 경우에는 화면에 로그가 출력 됩니다.
    - `spark_master` 접속해야할 spark master의 접속 정보를 설정합니다.

## spark 키워드

사용자는 기본적으로 `spark` 키워드를 이용하여 spark을 접속하여 분산 처리를 할 수 있습니다.

### spark

```
data = [1, 2, 3, 4, 5]
dist_data = spark._sc.parallelize(data)
dist_data.show()
```

위와 같이 `spark` 키워드를 이용하여 spark 을 이용한 연산을 처리할 수 있습니다.

### IRIS-on-Spark

```
irisDF = spark.read.format("iris")\
        .option("host", "192.168.100.180")\
        .option("version", "v2")\
        .option("user", "root" )\
        .option("passwd", "biris.manse")\
        .option("table", "EVA.SYSLOG")\
        .option("hint", "LOCATION(PARTITION > '20200901900000')")\
        .load()
irisDF.registerTempTable("angora")
data = spark.sql("select DATETIME, HOST from angora where (HOST='gcs0' or HOST!='gcs3') and PROGRAM!='CROND'")
data.show()
```

- spark을 통해 `IRIS-DB`에 존재하는 데이터를 가져올 경우 위와 같이 방식으로 데이터를 가져올수 있습니다.
- `spark.read.format("iris")` 의 옵션
  - `host`: `IRIS-DB` 의 마스터 노드의 주소를 입력합니다.
  - `version`: 현재는 `v2`로 고정해서 사용하시면 됩니다.
  - `user`,`passwd`: `IRIS-DB` 상의 계정 정보를 입력합니다.
  - `table`: 검색을 원하는 테이블 명을 입력합니다.
  - `hint`: 검색을 원하는 테이블의 검색 범위를 지정합니다.
    - `IRIS-DB`의 hint 구문과 동일 합니다.
- `spark.sql` 실제 분산되어 있는 블럭 파일에서 실행할 쿼리를 입력합니다. 해당 쿼리를 통해 실제 데이터를 블럭 파일에서 읽어 오게 됩니다.

##LOG 키워드

LOG 키워드는 로그를 출력할때 사용이 됩니다. LOG는 화면에 출력하는 방식과 파일로 저장하는 방식이 존재하며, 기본값은 화면에 출력이 됩니다.

만약 파일로 로그를 출력하고 싶을 경우에는 클래스를 실행시 아래와 같이 로그의 저장 경로를 지정해 주면 됩니다.

```
iris_spark = Test('TestApp', '/tmp/test.log')
```

파일로 저장되는 로그는 python의 `BaseRotatingHandler` 을 이용하게 됩니다. 따라서 로그 파일을 rotation을 하게 되며, 기본 설정값은 다음과 같습니다.

- 총 로그 파일수: 10
- 단일 로그 파일의 용량: 10,000,000 byte (= 10mb)

### 로그 출력

LOG 키워드를 이용하여 사용자가 임의의 로그를 출력할 수 있습니다.

    LOG.debug('debug message')
    LOG.info('info message')
    LOG.warn('warn message')
    LOG.error('error message')
    LOG.critical('critial message')
    LOG.exception(err)
각 로그는 레벨별로 출력이 가능합니다.

### 로그 레벨 정의

로그의 레벨은 아래와 같이 정의할 수 있습니다.

```
import IRISSPark
import logger

class Test(IRISSpark.Init):
    def process(self):
        self.set_log_level(logger.WARN)
```

python에서 제공되는 `logger` 를 import 하여 로그 레벨을 지정해 주시면 됩니다.


