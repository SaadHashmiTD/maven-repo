package com.thinkbiganalytics.veon.util;

public class Constants {

    /**
     * @author ar186004
     *
     * Event Store Layer-2 [L2] based constants
     */
    public static final class EVENT_STORE {
        public static final String EVENT_DATE				= "event_date";
        public static final String STREAM_TYPE				= "stream_type";
        public static final String EVENT_TIMESTAMP			= "event_timestamp";
        public static final String ORIGINATING_MSISDN		= "originating_msisdn";
        public static final String TERMINATING_MSISDN		= "terminating_msisdn";
        public static final String CALL_TYPE				= "call_type";
        public static final String OFFER					= "offer";
        public static final String USAGE_CLASS				= "usage_class";
        public static final String USAGE_CATEGORY			= "usage_category";
        public static final String SUB_OFFER				= "sub_offer";
        public static final String DOMESTIC_INDICATOR		= "domestic_indicator";
        public static final String CALL_DIRECTION			= "call_direction";
        public static final String DISCOUNT_INDICATOR		= "discount_indicator";
        public static final String LOCATION					= "location";
        public static final String OPERATOR_COUNTRY			= "operator_country";
        public static final String OPERATOR_NAME			= "operator_name";
        public static final String NETWORK_TYPE				= "network_type";
        public static final String RATING_GROUP				= "rating_group";
        public static final String PAYMENT_TYPE				= "payment_type";
        public static final String APPLICATION				= "application";
        public static final String SUB_APPLICATION			= "sub_application";
        public static final String PROTOCOL_CATEGORY		= "protocol_category";
        public static final String SUB_PROTOCOL_CATEGORY	= "sub_protocol_category";
        public static final String URL						= "url";
        public static final String BROWSER_TYPE				= "browser_type";
        public static final String TAC						= "tac";
        public static final String IMEI						= "imei";
        public static final String BVS_STATUS				= "bvs_status";
        public static final String SALE_TYPE				= "sale_type";
        public static final String CHANNEL_CODE				= "channel_code";
        public static final String ITEM_TYPE				= "item_type";
        public static final String EXPIRY_BEFORE			= "expiry_before";
        public static final String EXPIRY_AFTER				= "expiry_after";
        public static final String DATA_VOLUME_UP			= "data_volume_up";
        public static final String DATA_VOLUME_DOWN			= "data_volume_down";
        public static final String CALL_PULSE_CNT			= "call_pulse_cnt";
        public static final String CALL_DURATION			= "call_duration";
        public static final String CHARGE_DURATION			= "charge_duration";
        public static final String GROSS_REVENUE_AMOUNT		= "gross_revenue_amount";
        public static final String TOTAL_TAX_AMOUNT			= "total_tax_amount";
        public static final String ACCOUNT_CD				= "account_cd";
        public static final String MEASURE_QUANTITY			= "measure_quantity";
        public static final String MEASURE_AMOUNT			= "measure_amount";
        public static final String QUANTITY_UOM_CD			= "quantity_uom_cd";
        public static final String BALANCE_BEFORE			= "balance_before";
        public static final String BALANCE_AFTER			= "balance_after";
        public static final String JSON_EXTRA_DATA			= "json_extra_data";
        public static final String PROCESSING_DTTM			= "processing_dttm";

		/*
		 * Usage Example ...
		 *
		Dataset<Row> transformedDf = sourceDf.select(
			//.as("EVENT_DATE"),
			//.as("STREAM_TYPE"),
			//.as("EVENT_TIMESTAMP"),
			//.as("ORIGINATING_MSISDN"),
			//.as("TERMINATING_MSISDN"),
			//.as("CALL_TYPE"),
			//.as("OFFER"),
			//.as("USAGE_CLASS"),
			//.as("USAGE_CATEGORY"),
			//.as("SUB_OFFER"),
			//.as("DOMESTIC_INDICATOR"),
			//.as("CALL_DIRECTION"),
			//.as("DISCOUNT_INDICATOR"),
			//.as("LOCATION"),
			//.as("OPERATOR_COUNTRY"),
			//.as("OPERATOR_NAME"),
			//.as("NETWORK_TYPE"),
			//.as("RATING_GROUP"),
			//.as("PAYMENT_TYPE"),
			//.as("APPLICATION"),
			//.as("SUB_APPLICATION"),
			//.as("PROTOCOL_CATEGORY"),
			//.as("SUB_PROTOCOL_CATEGORY"),
			//.as("URL"),
			//.as("BROWSER_TYPE"),
			//.as("TAC"),
			//.as("IMEI"),
			//.as("BVS_STATUS"),
			//.as("SALE_TYPE"),
			//.as("CHANNEL_CODE"),
			//.as("ITEM_TYPE"),
			//.as("EXPIRY_BEFORE"),
			//.as("EXPIRY_AFTER"),
			//.as("DATA_VOLUME_UP"),
			//.as("DATA_VOLUME_DOWN"),
			//.as("CALL_PULSE_CNT"),
			//.as("CALL_DURATION"),
			//.as("CHARGE_DURATION"),
			//.as("GROSS_REVENUE_AMOUNT"),
			//.as("TOTAL_TAX_AMOUNT"),
			//.as("ACCOUNT_CD"),
			//.as("MEASURE_QUANTITY"),
			//.as("MEASURE_AMOUNT"),
			//.as("QUANTITY_UOM_CD"),
			//.as("BALANCE_BEFORE"),
			//.as("BALANCE_AFTER"),
			//.as("JSON_EXTRA_DATA"),
			lit(timestamp).as("processing_dttm")    //processing_dttm is always added in the feed initialization
		);
		 */
    }
}
