/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.core;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;
import org.bouncycastle.asn1.ASN1Integer;
import org.bouncycastle.asn1.ASN1OctetString;
import org.bouncycastle.asn1.ocsp.CertID;
import org.bouncycastle.asn1.x509.AlgorithmIdentifier;
import org.bouncycastle.operator.DigestCalculator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import static net.snowflake.client.jdbc.SnowflakeUtil.systemGetProperty;

class SSDManager
{
  private static final
  SFLogger LOGGER = SFLoggerFactory.getLogger(SSDManager.class);

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static String SF_SSD_CACHE;
  private static SSDKeyManager pub_key_dep1 = new SSDKeyManager();
  private static SSDKeyManager pub_key_dep2 = new SSDKeyManager();

  private static final String keyUpdDirEnvVariable = "SF_KEY_UPD_SSD";
  private static final String hostSpecBypassEnvVariable = "SF_HOST_SPEC_BYPASS_SSD";

  private static KeyUpdSSD keyUpdateSSD;
  private static HostSpecSSD hostSpecBypassSSD;

  private static boolean ACTIVATE_SSD;

  SSDManager()
  {
    String ssd_status = null;
    String key_upd_ssd = null;
    String host_spec_ssd = null;
    try
    {
      ssd_status = System.getenv("SF_OCSP_ACTIVATE_SSD");
      if (ssd_status != null)
      {
        key_upd_ssd = System.getenv(keyUpdDirEnvVariable);
        host_spec_ssd = System.getenv(hostSpecBypassEnvVariable);
      }
    }
    catch (Throwable ex)
    {
      LOGGER.debug("Failed to get environment variable for Server Side Directive support");
    }

    if (ssd_status == null)
    {
      ssd_status = systemGetProperty("net.snowflake.jdbc.ssd_support_enabled");
      if (ssd_status != null)
      {
        key_upd_ssd = systemGetProperty("net.snowflake.jdbc.key_upd_ssd");
        host_spec_ssd = systemGetProperty("net.snowflake.jdbc.host_spec_ssd");
      }
    }

    SSDManager.ACTIVATE_SSD = Boolean.TRUE.toString().equalsIgnoreCase(ssd_status);

    if (SSDManager.ACTIVATE_SSD)
    {
      this.clearSSDCache();
      /*
       * Initialize in memory pub key to
       * packaged public keys
       */
      pub_key_dep1.SSD_setKey(SSDPubKey.getPublicKeyInternal("dep1"), 0.1);
      pub_key_dep2.SSD_setKey(SSDPubKey.getPublicKeyInternal("dep2"), 0.1);

      try
      {
        if (key_upd_ssd != null)
        {
          keyUpdateSSD = new KeyUpdSSD();
        }

        if (host_spec_ssd != null)
        {
          hostSpecBypassSSD = new HostSpecSSD();
        }

        JsonNode jnode_key_upd = OBJECT_MAPPER.readTree(key_upd_ssd);
        if (jnode_key_upd.has("dep1"))
        {
          keyUpdateSSD.setIssuer("dep1");
          keyUpdateSSD.setKeyUpdDirective(jnode_key_upd.get("dep1").textValue());
        }
        else
        {
          keyUpdateSSD.setIssuer("dep2");
          keyUpdateSSD.setKeyUpdDirective(jnode_key_upd.get("dep2").textValue());
        }

        JsonNode jnode_host_spec = OBJECT_MAPPER.readTree(host_spec_ssd);
        Map<?, ?> keyVal = OBJECT_MAPPER.readValue(host_spec_ssd, HashMap.class);
        Iterator<?> itr = keyVal.keySet().iterator();
        while (itr.hasNext())
        {
          String key_val = (String) itr.next();
          String ssd_val = jnode_host_spec.get(key_val).textValue();
          hostSpecBypassSSD.setHostname(key_val);
          hostSpecBypassSSD.setHostSpecDirective(ssd_val);
        }
      }
      catch (Throwable ex)
      {
        LOGGER.debug("Could not read JSON from the directive passed.");
      }
    }
  }

  boolean getSSDSupportStatus()
  {
    return SSDManager.ACTIVATE_SSD;
  }

  void addToSSDCache(String host_spec_ssd)
  {
    SF_SSD_CACHE = host_spec_ssd;
  }

  void clearSSDCache()
  {
    SF_SSD_CACHE = null;
  }

  String getSSDFromCache()
  {
    return SF_SSD_CACHE;
  }

  void updateKey(String dep, String pub_key, float ver)
  {
    if (dep.equals("dep1"))
    {
      pub_key_dep1.SSD_setKey(pub_key, ver);
    }
    else if (dep.equals("dep2"))
    {
      pub_key_dep2.SSD_setKey(pub_key, ver);
    }

    LOGGER.debug("Failed to update public key, unknown issuing deployment");
  }

  String getPubKey(String dep)
  {
    if (dep.equals("dep1"))
    {
      return pub_key_dep1.SSD_getKey();
    }

    else if (dep.equals("dep2"))
    {
      return pub_key_dep2.SSD_getKey();
    }

    LOGGER.debug("Invalid deployment name");
    return null;
  }

  double getPubKeyVer(String dep)
  {
    if (dep.equals("dep1"))
    {
      return pub_key_dep1.SSD_getKeyVer();
    }

    else if (dep.equals("dep2"))
    {
      return pub_key_dep2.SSD_getKeyVer();
    }

    LOGGER.debug("Invalid deployment name");
    return -1;
  }

  /**
   * Getters for Out of Band SSDs
   * No setters as these should only
   * be set by system property or
   * by environment variables.
   */
  KeyUpdSSD getKeyUpdateSSD()
  {
    return keyUpdateSSD;
  }

  HostSpecSSD getHostSpecBypassSSD()
  {
    return hostSpecBypassSSD;
  }

  SFTrustManager.OcspResponseCacheKey getWildCardCertId()
  {
    DigestCalculator digest = new SFTrustManager.SHA1DigestCalculator();
    AlgorithmIdentifier algo = digest.getAlgorithmIdentifier();
    ASN1OctetString nameHash = ASN1OctetString.getInstance("0");
    ASN1OctetString keyHash = ASN1OctetString.getInstance("0");
    ASN1Integer serial_number = ASN1Integer.getInstance(0);
    CertID cid = new CertID(algo, nameHash, keyHash, serial_number);
    SFTrustManager.OcspResponseCacheKey keyOcspResp = null;
    try
    {
      keyOcspResp = new SFTrustManager.OcspResponseCacheKey(
          ASN1OctetString.getInstance("0").getEncoded(),
          ASN1OctetString.getInstance("0").getEncoded(),
          ASN1Integer.getInstance(0).getValue());
    }
    catch (Throwable ex)
    {
      LOGGER.debug("Could not create wildcard certid as cache key");
      keyOcspResp = null;
    }
    return keyOcspResp;
  }
}