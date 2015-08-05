/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.geoapi.dataflownodes.shapefileconverterdataprocessor;

import java.io.File;
import java.util.Collections;
import java.util.UUID;
import org.junit.Test;
import com.arjuna.databroker.data.DataProcessor;
import com.arjuna.databroker.data.connector.ObservableDataProvider;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.databroker.data.core.DataFlowNodeLifeCycleControl;
import com.arjuna.dbplugins.geoapi.dataflownodes.ShapeFileConverterDataProcessor;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSink;
import com.arjuna.dbutils.testsupport.dataflownodes.dummy.DummyDataSource;
import com.arjuna.dbutils.testsupport.dataflownodes.lifecycle.TestJEEDataFlowNodeLifeCycleControl;

public class ConverterTest
{
    @Test
    public void simplestConversion()
    {
        DataFlowNodeLifeCycleControl dataFlowNodeLifeCycleControl = new TestJEEDataFlowNodeLifeCycleControl();

        DummyDataSource dummyDataSource        = new DummyDataSource("Dummy Data Source", Collections.<String, String>emptyMap());
        DataProcessor   shapeFileDataProcessor = new ShapeFileConverterDataProcessor("ShapeFile Converter Data Processor", Collections.<String, String>emptyMap());
        DummyDataSink   dummyDataSink          = new DummyDataSink("Dummy Data Sink", Collections.<String, String>emptyMap());

        dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSource, null);
        dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), shapeFileDataProcessor, null);
        dataFlowNodeLifeCycleControl.completeCreationAndActivateDataFlowNode(UUID.randomUUID().toString(), dummyDataSink, null);

        ((ObservableDataProvider<File>) dummyDataSource.getDataProvider(File.class)).addDataConsumer((ObserverDataConsumer<File>) shapeFileDataProcessor.getDataConsumer(File.class));
        ((ObservableDataProvider<String>) shapeFileDataProcessor.getDataProvider(String.class)).addDataConsumer((ObserverDataConsumer<String>) dummyDataSink.getDataConsumer(String.class));

        File testFile = new File("/tmp/Gully_point/Gully_point.shp");
        dummyDataSource.sendData(testFile);

        dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSource);
        dataFlowNodeLifeCycleControl.removeDataFlowNode(shapeFileDataProcessor);
        dataFlowNodeLifeCycleControl.removeDataFlowNode(dummyDataSink);

    }
}
