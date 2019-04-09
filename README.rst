Snowflake JDBC Driver
*********************

.. image:: https://travis-ci.org/snowflakedb/snowflake-jdbc.svg?branch=master
    :target: https://travis-ci.org/snowflakedb/snowflake-jdbc

.. image:: http://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt
    
.. image:: https://maven-badges.herokuapp.com/maven-central/net.snowflake/snowflake-jdbc/badge.svg?style=plastic
    :target: http://repo2.maven.org/maven2/net/snowflake/snowflake-jdbc/

.. image:: https://codecov.io/gh/snowflakedb/snowflake-jdbc/branch/master/graph/badge.svg
    :target: https://codecov.io/gh/snowflakedb/snowflake-jdbc
    
Snowflake provides a JDBC type 4 driver that supports core functionality, allowing Java program to connect to Snowflake.

Prerequisites
=============

The Snowflake JDBC driver requires Java 1.8 or higher. If the minimum required version of Java is not installed on the client machines where the JDBC driver is installed, you must install either Oracle Java or OpenJDK.

Installation
============

Maven
-----
Add following code block as a dependency

.. code-block:: xml

    <dependency>
      <groupId>net.snowflake</groupId>
      <artifactId>snowflake-jdbc</artifactId>
      <version>{version}</version>
    </dependency>


Build from Source Code 
----------------------
1. Checkout source code from Github by running:

.. code-block:: bash

    git clone https://github.com/snowflakedb/snowflake-jdbc.git

2. Build the driver by running:

.. code-block:: bash

    mvn install

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

Follow these steps to apply the coding style specified in ``intellij-codestyle.xml``.

- Download the IntelliJ IDEA from https://www.jetbrains.com/idea/download/ if the software is not already installed.
- Ensure IntelliJ is not running.
- Run the following command:

.. code-block:: bash

    $INTELLIJ_HOME/bin/format.sh -mask "*.java" -settings intellij-codestyle.xml -R src

where ``INTELLIJ_HOME`` refers to the IntelliJ directory.

You may import the coding style from IntelliJ so that the coding style can be applied on IDE:

- In the **File** -> **Settings/Preferences**, and then **Code Style** -> **Java**.
- Click the gear icon to select **Import Scheme**.
- Select ``intellij-codestyle.xml`` to set the schema.
- In the source code window, select **Code** -> **Reformat** to apply the coding style.


Support
=============

Feel free to file an issue or submit a PR here for general cases. For official support, contact Snowflake support at:
https://community.snowflake.com/s/article/How-To-Submit-a-Support-Case-in-Snowflake-Lodge
