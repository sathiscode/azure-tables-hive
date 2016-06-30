package com.microsoft.hadoop.azure.oldinterface;

import static com.microsoft.hadoop.azure.AzureTableConfiguration.*;

import java.io.*;
import java.util.ArrayList;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

import com.microsoft.hadoop.azure.*;
import com.microsoft.azure.storage.*;
import com.microsoft.azure.storage.table.*;
import org.apache.commons.lang.exception.ExceptionUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An input format using the deprecated mapred.* API for reading Azure Tables.
 * Implemented here since HiveStorageHandler uses the old API.
 */
public class OldAzureTableInputFormat implements InputFormat<Text, WritableEntity> {

    private static final Log LOG = LogFactory.getLog(OldAzureTableInputFormat.class);

    @Override
    public RecordReader<Text, WritableEntity> getRecordReader(InputSplit split,
            JobConf job, Reporter reporter) throws IOException {
        return new OldAzureTableReader((WrapperSplit) split, job);
    }

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) throws IOException {
        AzureTablePartitioner partitioner = getPartitioner(job);
        CloudTable table = getTableReference(job);
        ArrayList<InputSplit> ret = new ArrayList<InputSplit>();
        Path[] tablePaths = FileInputFormat.getInputPaths(job);
        try {
            for (AzureTableInputSplit split : partitioner.getSplits(table)) {
                ret.add(new WrapperSplit(split, tablePaths[0], job));
            }
        } catch (StorageException e) {
            System.out.println("Sathis");
            System.out.println(ExceptionUtils.getFullStackTrace(e));
            System.out.println(ExceptionUtils.getRootCauseMessage(e));
            System.out.println(ExceptionUtils.getStackTrace(e));
            System.err.println("Sathis");
            System.err.println(ExceptionUtils.getFullStackTrace(e));
            System.err.println(ExceptionUtils.getRootCauseMessage(e));
            System.err.println(ExceptionUtils.getStackTrace(e));
            LOG.error(e);
            throw new IOException(e);
        }
        return ret.toArray(new InputSplit[0]);
    }
}
