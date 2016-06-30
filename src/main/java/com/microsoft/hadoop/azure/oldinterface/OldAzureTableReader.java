package com.microsoft.hadoop.azure.oldinterface;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.createTableClient;
import static com.microsoft.hadoop.azure.AzureTableConfiguration.getTableName;

import java.io.*;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.microsoft.hadoop.azure.*;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A record reader using the old mapred.* API that reads entities from Azure
 * Tables.
 */
public class OldAzureTableReader implements RecordReader<Text, WritableEntity> {

    private static final Log LOG = LogFactory.getLog(OldAzureTableReader.class);
    private Iterator<WritableEntity> queryResults;
    private long pos;

    /**
     * Create the reader for the given split.
     */
    OldAzureTableReader(WrapperSplit split, Configuration conf) throws IOException {
        CloudTableClient tableClient = createTableClient(conf);
        String tableName = getTableName(conf);
        TableQuery<WritableEntity> query = split.getWrappedSplit().getQuery();
        try {
            queryResults = tableClient.getTableReference(tableName).execute(query).iterator();
        } catch (StorageException e) {
            LOG.error(e);
            throw new IOException(e);
        } catch (URISyntaxException e) {
            LOG.error(e);
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Text createKey() {
        return new Text();
    }

    @Override
    public WritableEntity createValue() {
        return new WritableEntity();
    }

    @Override
    public long getPos() throws IOException {
        return pos;
    }

    @Override
    public float getProgress() throws IOException {
        // No idea.
        return 0.5f;
    }

    @Override
    public boolean next(Text key, WritableEntity value) throws IOException {
        if (queryResults.hasNext()) {
            WritableEntity obtained = queryResults.next();
            key.set(obtained.getRowKey());
            value.setPartitionKey(obtained.getPartitionKey());
            value.setRowKey(obtained.getRowKey());
            value.setTimestamp(obtained.getTimestamp());
            value.setProperties(obtained.getProperties());
            pos++;
            return true;
        } else {
            return false;
        }
    }
}
