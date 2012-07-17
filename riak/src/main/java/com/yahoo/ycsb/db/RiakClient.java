package com.yahoo.ycsb.db;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.codehaus.jackson.map.ObjectMapper;

import com.basho.riak.client.IRiakClient;
import com.basho.riak.client.RiakException;
import com.basho.riak.client.RiakFactory;
import com.basho.riak.client.RiakRetryFailedException;
import com.basho.riak.client.bucket.Bucket;
import com.basho.riak.client.cap.UnresolvedConflictException;
import com.basho.riak.client.convert.ConversionException;
import com.basho.riak.client.raw.pbc.PBClientConfig;

import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.StringByteIterator;

/**
 * Riak client for YCSB framework.
 *
 * Properties to set:
 *
 * riak.host=localhost
 * riak.port=8087
 * riak.bucket=ycsb
 *
 * @author jbohman
 *
 */
public class RiakClient extends DB {

    private IRiakClient client;
    private Bucket bucket;

    /**
     * Initialize any state for this DB. Called once per DB instance; there is
     * one DB instance per client thread.
     */
    public void init() throws DBException {
        Properties props = getProperties();
        String host = props.getProperty("riak.host", "");
        String port = props.getProperty("riak.port", "8087");
        String bucket_name = props.getProperty("riak.bucket", "ycsb");

        PBClientConfig conf = new PBClientConfig.Builder().withHost(host).withPort(Integer.parseInt(port)).build();

        try {
            client = RiakFactory.newClient(conf);
            bucket = client.createBucket(bucket_name).execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int delete(String table, String key) {
        String new_key = table.concat(key);
        try {
            bucket.delete(new_key).execute();
        } catch (RiakException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return 1;
        } // dw??

        return 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    /**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
    public int read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
        String new_key = table.concat(key);
        //if (fields == null) {
            ObjectMapper mapper = new ObjectMapper();
            try {
                StringByteIterator.putAllAsByteIterators(result, mapper.readValue(bucket.fetch(new_key).execute().getValue(), Map.class));
            } catch (RiakException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return 1;
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return 1;
            }
        //} else {}

        return result.isEmpty() ? 1 : 0;
    }

    @Override
    /**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int insert(String table, String key, HashMap<String, ByteIterator> values) {
        String new_key = table.concat(key);
        try {
            bucket.store(new_key, StringByteIterator.getStringMap(values)).execute();
        } catch (RiakException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

    @SuppressWarnings("unchecked")
    @Override
    /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int update(String table, String key, HashMap<String, ByteIterator> values) {
        String new_key = table.concat(key);

        // Read current data first
        ObjectMapper mapper = new ObjectMapper();
        HashMap<String, ByteIterator> result = new HashMap<String, ByteIterator>();
        try {
            StringByteIterator.putAllAsByteIterators(result, mapper.readValue(bucket.fetch(new_key).execute().getValue(), Map.class));
        } catch (RiakException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return 1;
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return 1;
        }

        // Update hashmap with new values
        result.putAll(values);

        // Write data to bucket
        try {
            bucket.store(new_key, StringByteIterator.getStringMap(result)).execute();
        } catch (RiakException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return 1;
        }

        return 0;
    }

    @Override
    /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
    public int scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
        //String new_key = table.concat(startkey);
        return 1;
    }
}
