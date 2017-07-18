package com.thinkbiganalytics.veon.util;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class ApplicationLogger {

    /**
     * Default Constructor to prepare Logger.
     */
    private ApplicationLogger() {
        BasicConfigurator.configure();
    }


    /**
     * @param appLoggerName: Application Name
     * @return instance of logger
     */
    public static Logger getLoggerInstance(String appLoggerName) {
        return Logger.getLogger(appLoggerName);
    }

}
