/*
 * Copyright (c) 2012-2019 Snowflake Computing Inc. All rights reserved.
 */

package net.snowflake.client.loader;

import java.io.IOException;

import net.snowflake.client.log.SFLogger;
import net.snowflake.client.log.SFLoggerFactory;

/**
 * Queue that sequentially finalizes BufferStage uploads and schedules them for
 * processing in ProcessQueue.
 */
public class PutQueue implements Runnable
{
  private static final SFLogger LOGGER = SFLoggerFactory.getLogger(
      PutQueue.class);

  private final Thread _thread;

  private final StreamLoader _loader;

  public PutQueue(StreamLoader loader)
  {
    LOGGER.debug("");
    _loader = loader;
    _thread = new Thread(this);
    _thread.setName("PutQueueThread");
    _thread.start();
  }

  @Override
  public void run()
  {

    while (true)
    {

      BufferStage stage = null;

      try
      {

        stage = _loader.takePut();

        if (stage.getRowCount() == 0)
        {
          // Nothing was written to that stage
          if (stage.isTerminate())
          {
            _loader.queueProcess(stage);
            stage.completeUploading();
            break;
          }
          else
          {
            continue;
          }
        }

        // Uploads the stage
        stage.completeUploading();

        // Schedules it for processing
        _loader.queueProcess(stage);

        if (stage.isTerminate())
        {
          break;
        }

      }
      catch (InterruptedException | IOException ex)
      {
        LOGGER.error("Exception: ", ex);
        break;
      }
      finally
      {

      }
    }
  }

  public void join()
  {
    try
    {
      _thread.join(0);
    }
    catch (InterruptedException ex)
    {
      LOGGER.error("Exception: ", ex);
    }
  }

}
