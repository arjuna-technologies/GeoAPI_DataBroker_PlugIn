/*
 * Copyright (c) 2014, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.tests.geoapi.dataflownodes.shapefileconverterdataprocessor;

import java.io.File;
import java.util.Collections;
import org.junit.Test;
import static org.junit.Assert.*;
import com.arjuna.databroker.data.connector.ObserverDataConsumer;
import com.arjuna.dbplugins.geoapi.dataflownodes.ShapeFileConverterDataProcessor;

public class ConverterTest
{
    @Test
    public void simplestConversion()
    {
        ShapeFileConverterDataProcessor shapeFileDataProcessor = new ShapeFileConverterDataProcessor("ShapeFile Converter Data Processor", Collections.<String, String>emptyMap());
        ObserverDataConsumer<File>      dataConsumer           = (ObserverDataConsumer<File>) shapeFileDataProcessor.getDataConsumer(File.class);

        File testFile = new File("/tmp/Gully_point/Gully_point.shp");

        dataConsumer.consume(null, testFile);
    }
}
