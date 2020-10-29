/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */
package net.snowflake.client.core;

class SSDKeyManager
{
  private String pub_key;
  private double pub_key_ver;

  SSDKeyManager()
  {
    this.pub_key = null;
    this.pub_key_ver = 0;
  }

  void SSD_setKey(String pub_key, double pub_key_ver)
  {
    this.pub_key = pub_key;
    this.pub_key_ver = pub_key_ver;
  }

  String SSD_getKey()
  {
    return this.pub_key;
  }

  double SSD_getKeyVer()
  {
    return this.pub_key_ver;
  }
}