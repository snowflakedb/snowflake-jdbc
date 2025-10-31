package net.snowflake.client.common.core;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import java.util.Set;
import net.snowflake.client.core.SnowflakeJdbcInternalApi;

/**
 * Handle error messages.
 *
 * <p>This class encapsulate error message localization using Java bundle.
 *
 * @author tcruanes
 */
@SnowflakeJdbcInternalApi
public class ResourceBundleManager {
  private static Map<String, ResourceBundleManager> resourceManagers =
      new HashMap<String, ResourceBundleManager>();

  private static final Object lockObject = new Object();

  // The name of the Snowflake message file
  private final String bundleName;
  private ResourceBundle resourceBundle;

  private ResourceBundleManager(String bundleName) {
    this.bundleName = bundleName;

    // just call reload to load the resource bundle initially
    reload();
  }

  /** Reload the error message bundle based on the current locale. */
  public void reload() {
    ResourceBundle bundle = null;

    // Try to load the localized bundle, if it was not found or an error was
    // raised, try to load the default (english) version of the bundle that
    // should always be there.
    try {
      // try to get localized version
      bundle = ResourceBundle.getBundle(bundleName, Locale.getDefault());
    } catch (Throwable ex1) {
      try {
        // Try to load the general bundle (without locale, US)
        bundle = ResourceBundle.getBundle(bundleName);
      } catch (Throwable ex2) {
        throw new RuntimeException(
            "Can't load localized resource bundle due to "
                + ex1.toString()
                + " and can't load default resource bundle due to "
                + ex2.toString());
      }
    } finally {
      resourceBundle = bundle;
    }
  }

  /**
   * Returns the localized error message for the given message key
   *
   * @param key the message key
   * @return The localized message for the key
   */
  public String getLocalizedMessage(String key) {
    // Make sure that the message resource bundle was loaded.
    if (resourceBundle == null) {
      throw new RuntimeException(
          "Localized messages from resource bundle '" + bundleName + "' not loaded.");
    }

    // Try to find the corresponding key
    try {
      if (key == null) {
        throw new IllegalArgumentException("Message key cannot be null");
      }

      String message = resourceBundle.getString(key);

      // Message not found in the bundle,
      if (message == null) {
        message = "!!" + key + "!!";
      }

      return message;
    } catch (MissingResourceException e) {
      return '!' + key + '!';
    }
  }

  /**
   * Returns the localized error message for the given message key and use the arguments to returned
   * a formatted result string.
   *
   * @param key message key
   * @param args arguments of message
   * @return a localized message
   */
  public String getLocalizedMessage(String key, Object... args) {
    return MessageFormat.format(getLocalizedMessage(key), args);
  }

  /**
   * @return Return the set of error messages for the current locale
   */
  public ResourceBundle getResourceBundle() {
    return resourceBundle;
  }

  public static ResourceBundleManager getSingleton(String bundleName) {
    if (resourceManagers.get(bundleName) != null) {
      return resourceManagers.get(bundleName);
    }

    synchronized (lockObject) {
      if (resourceManagers.get(bundleName) != null) {
        return resourceManagers.get(bundleName);
      }

      resourceManagers.put(bundleName, new ResourceBundleManager(bundleName));
    }

    return resourceManagers.get(bundleName);
  }

  public Set<String> getKeySet() {
    return resourceBundle.keySet();
  }
}
