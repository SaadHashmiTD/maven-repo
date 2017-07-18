package com.thinkbiganalytics.veon.util;

import org.apache.spark.sql.api.java.UDF3;

public class TransformationFunctions {

    /**
     * Derive location based on CGI, SAI, ECGI from DPI Source
     */
    public static UDF3<String, String, String, String> location = new UDF3<String, String, String, String>() {

		private static final long serialVersionUID = 1L;

		/**
         * @param cgi
         * @param sai
         * @param ecgi
         * @return
         * @throws Exception
         */
        public String call(String cgi, String sai, String ecgi) throws Exception {
            if (cgi != null && cgi.length() >= 14) {

                String lacid = Integer.valueOf(cgi.substring(6, 10), 16).toString();
                String cellid = String.format("%05d", Integer.valueOf(cgi.substring(10, 14), 16));

                return lacid + cellid.substring(1, 5);

            } else if (sai != null && sai.length() >= 14) {

                String lacid = Integer.valueOf(sai.substring(6, 10), 16).toString();
                String cellid = String.format("%04d", Integer.valueOf(sai.substring(10, 14), 16));

                return lacid + cellid.substring(0, 4);

            } else if (ecgi != null && ecgi.length() >= 11) {

                String cellid = Integer.valueOf(ecgi.substring(6, 11), 16).toString();

                return cellid;
            } else return null;
        }
    };
}

/*
ORIGINAL LOCATION QUERY


"select concat(coalesce(lacid,''),case when ind =1 then substring(cellid,2,4) when ind=2 then substring(cellid,1,4) when ind=3 then cellid end) as location
from
(
select
conv(lacid,16,10) as lacid,
ind,
conv(cellid,16,10) as cellid
from
(
select
case when cgi is not null and cgi <>'' then  substring(cgi,7,4) when sai is not null and sai <>'' then  substring(sai,7,4) when ecgi is not null and ecgi <>'' then  null end as lacid,
case when cgi is not null and cgi <>'' then  substring(cgi,11,4) when sai is not null and sai <>'' then  substring(sai,11,4) when ecgi is not null and ecgi <>'' then  substring(ecgi,7,5)  end as cellid,
case when cgi is not null and cgi <>'' then  1 when sai is not null and sai <>'' then  2 when ecgi is not null and ecgi <>'' then  3  end as ind
 from dd_dpi_stg.dpi_udn
 limit 10
) as a
) as b
;"
 */
