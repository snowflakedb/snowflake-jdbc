Test Only Maven Project
***********************

This directory includes a maven project to run the Snowflake JDBC tests with the specified JDBC version.
The primary goal is to run the tests against the oldest support JDBC driver to ensure no regression.

Run Tests
==========

.. code-block:: bash

    mvn -DjenkinsIT -DtestCategory=net.snowflake.client.category.<category> verify

where ``category`` is the class name under the package ``net.snowflake.client.category``.

Update the JDBC version
=======================

Here are the steps updating the target JDBC driver version.

- Change the project version in ``pom.xml`` to the JDBC version that you want to run the tests.
- Locate ``maven-compiler-plugin`` plugin in ``pom.xml``
- Delete test case class files that should run along with the JDBC version.
- Check ``*LatestIT.java`` and move the test cases that should run along with the JDBC to the base classes.
