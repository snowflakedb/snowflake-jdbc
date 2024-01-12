Arrow dependencies are built from internal branch `upgradeTo10.0.2` and should have version 10.0.2.
On maven central there is no such version of arrow artifacts and maven build fails at the beginning of parsing pom, when dependencies are downloaded.
We need to keep old version `10.0.1` and let maven to download artifacts first and then we can replace them in the build process.