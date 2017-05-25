Snowflake JDBC Driver
*********************

.. image:: http://img.shields.io/:license-Apache%202-brightgreen.svg
    :target: http://www.apache.org/licenses/LICENSE-2.0.txt

Snowflake provides a JDBC type 4 driver that supports core functionality, allowing Java program to connect to Snowflake.

Prerequisites
=============

The Snowflake JDBC driver requires Java 1.7 or higher. If the minimum required version of Java is not installed on the client machines where the JDBC driver is installed, you must install either Oracle Java or OpenJDK.

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

    git clone git@github.com:snowflakedb/snowflake-jdbc

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
