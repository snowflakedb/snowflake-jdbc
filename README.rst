Snowflake JDBC Driver
*********************

.. image:: https://github.com/snowflakedb/snowflake-jdbc/workflows/Build%20and%20Test/badge.svg?branch=master
      :target: https://github.com/snowflakedb/snowflake-jdbc/actions?query=workflow%3A%22Build+and+Test%22+branch%3Amaster

.. image:: https://codecov.io/gh/snowflakedb/snowflake-jdbc/branch/master/graph/badge.svg?token=Mj6uPxk0pV
     :target: https://codecov.io/gh/snowflakedb/snowflake-jdbc

.. image:: http://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt

Snowflake provides a JDBC type 4 driver that supports core functionality, allowing Java program to connect to Snowflake.

.. |maven-snowflake-jdbc| image:: https://maven-badges.herokuapp.com/maven-central/net.snowflake/snowflake-jdbc/badge.svg?style=plastic
    :target: https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc/

.. |maven-snowflake-jdbc-fips| image:: https://maven-badges.herokuapp.com/maven-central/net.snowflake/snowflake-jdbc-fips/badge.svg?style=plastic
    :target: https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc-fips/

.. |maven-snowflake-jdbc-thin| image:: https://maven-badges.herokuapp.com/maven-central/net.snowflake/snowflake-jdbc-thin/badge.svg?style=plastic
    :target: https://repo1.maven.org/maven2/net/snowflake/snowflake-jdbc-thin/

- snowflake-jdbc (fat-jar): |maven-snowflake-jdbc|
- snowflake-jdbc-fips (FIPS compliant fat-jar): |maven-snowflake-jdbc-fips|
- snowflake-jdbc-thin (thin-jar): |maven-snowflake-jdbc-thin|

Prerequisites
=============

The Snowflake JDBC driver requires Java 1.8 or higher. If the minimum required version of Java is not installed on the client machines where the JDBC driver is installed, you must install either Oracle Java or OpenJDK.

Installation
============

Maven
-----
Add following dependency for fat-jar

.. code-block:: xml

    <dependency>
      <groupId>net.snowflake</groupId>
      <artifactId>snowflake-jdbc</artifactId>
      <version>{version}</version>
    </dependency>

or for FIPS compliant fat-jar

.. code-block:: xml

    <dependency>
      <groupId>net.snowflake</groupId>
      <artifactId>snowflake-jdbc-fips</artifactId>
      <version>{version}</version>
    </dependency>

or for thin-jar

.. code-block:: xml

    <dependency>
      <groupId>net.snowflake</groupId>
      <artifactId>snowflake-jdbc-thin</artifactId>
      <version>{version}</version>
    </dependency>

Build from Source Code 
----------------------
1. Checkout source code from Github by running:

.. code-block:: bash

    git clone https://github.com/snowflakedb/snowflake-jdbc.git

2. Build the fat-jar and install it in local maven repository by running:

.. code-block:: bash

    ./mvnw clean verify
    ./mvnw org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file -Dfile=target/snowflake-jdbc.jar -DpomFile=./public_pom.xml

3. Build the FIPS compliant fat-jar and install it in local maven repository by running:

.. code-block:: bash

    cd FIPS
    ../mvnw clean verify
    ../mvnw org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file -Dfile=target/snowflake-jdbc-fips.jar -DpomFile=./public_pom.xml
    cd -

4. Build the thin-jar and install it in local maven repository by running:

.. code-block:: bash

    ./mvnw clean verify -Dnot-self-contained-jar -Dthin-jar
    ./mvnw org.apache.maven.plugins:maven-install-plugin:3.1.1:install-file -Dfile=target/snowflake-jdbc-thin.jar -DpomFile=./thin_public_pom.xml -Dnot-self-contained-jar -Dthin-jar

- ``thin-jar`` enables thin jar profile
- ``not-self-contained-jar`` turns off fat jar profile (enabled by default)

5. **Note that the built dependencies are installed with version 1.0-SNAPSHOT**

Usage
=====

Load Driver Class
-----------------

.. code-block:: java

    Class.forName("net.snowflake.client.jdbc.SnowflakeDriver")

Datasource
----------

javax.sql.DataSource interface is implemented by class

.. code-block:: java

    net.snowflake.client.jdbc.SnowflakeBasicDataSource

Connection String
-----------------

US(West) Region:

.. code-block:: bash

    jdbc:snowflake://<account>.snowflakecomputing.com/?<connection_params>


EU(Frankfurt) Region:

.. code-block:: bash

    jdbc:snowflake://<account>.eu-central-1.snowflakecomputing.com/?<connection_params>

Documentation
=============

For detailed documentation, please refer to https://docs.snowflake.net/manuals/user-guide/jdbc.html

Development
=============

Run the maven command to check the coding style.

.. code-block:: bash

    mvn -P check-style validate

Follow the instruction if any error occurs or run this command to fix the formats.

.. code-block:: bash

    mvn com.spotify.fmt:fmt-maven-plugin:format

You may import the coding style from IntelliJ so that the coding style can be applied on IDE:

- In the **File** -> **Settings/Plugins**, and install `google-java-format` plugin.
- Enable `google-java-format` for the JDBC project.
- In the source code window, select **Code** -> **Reformat** to apply the coding style.
- Additionally configure IDE in **File** -> **Editor** -> **Code Style** -> **Java** to
  - not use wildcard imports (tab **Imports**):
    - **Use single class import**
    - **Class count to use import with '*'** to 1000
    - **Names count to use static import with '*'** to 1000
  - always use braces in ``if/while/for/do..while`` in (tab **Wrapping and Braces**)

Tests
=====

Run Tests
---------

Set the environment variables to specify the target database.

.. code-block:: bash

    export SNOWFLAKE_TEST_HOST=<your_host>
    export SNOWFLAKE_TEST_ACCOUNT=<your_account>
    export SNOWFLAKE_TEST_USER=<your_user>
    export SNOWFLAKE_TEST_PASSWORD=<your_password>
    export SNOWFLAKE_TEST_DATABASE=<your_database>
    export SNOWFLAKE_TEST_SCHEMA=<your_schema>
    export SNOWFLAKE_TEST_WAREHOUSE=<your_warehouse>
    export SNOWFLAKE_TEST_ROLE=<your_role>

Run the maven ``verify`` goal.

.. code-block:: bash

    mvn -DjenkinsIT -DtestCategory=net.snowflake.client.category.<category> verify

where ``category`` is the class name under the package ``net.snowflake.client.category``.

Prepare new version
---------------

Run script passing desired version:

.. code-block:: bash

   ./prepareNewVersion.sh 3.100.42

Add SNAPSHOT suffix when necessary:

.. code-block:: bash

   ./prepareNewVersion.sh 3.100.42-SNAPSHOT

Test Class Naming Convention
----------------------------

The test cases are fallen into a couple of criteria:

- The unit test class names end with ``Test``. They run part of the JDBC build jobs.
- The integration test class names end with ``IT``. They run part of the ``verify`` maven goal along with the test category specified by the parameter ``testCategory`` having ``net.snowflake.client.category`` classes.
- The manual test class names end with ``Manual``. They don't run in the CI but you can run them manually.

Aside from the general test criteria, the test case class names ending with ``LatestIT`` run only with the latest JDBC driver.
The main motivation behind is to skip those tests for the old JDBC driver. See ``./TestOnly`` directory for further information.

Support
=============

Feel free to file an issue or submit a PR here for general cases. For official support, contact Snowflake support at:
https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge

Note
----------

This driver support GCP regional endpoints starting from version 3.21.0. Please ensure that any workloads using through this driver below the version 3.21.0 do not require support for regional endpoints on GCP. If you have questions about this, please contact Snowflake Support.
