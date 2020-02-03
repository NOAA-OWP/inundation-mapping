/*  
 Copyright (C) 2016  
 National Center for Supercomputing Applications (NCSA)
 University of Illinois at Urbana-Champaign

This program is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License 
version 2, 1991 as published by the Free Software Foundation.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

A copy of the full GNU General Public License is included in file 
gpl.html. This is also available at:
http://www.gnu.org/copyleft/gpl.html
or from:
The Free Software Foundation, Inc., 59 Temple Place - Suite 330, 
Boston, MA  02111-1307, USA. 
 */

#include <cstdlib>
#include <string>
#include <iostream>
#include <vector>
#include <algorithm>
#include <unordered_set>

#include "gdal.h"
#include "gdal_priv.h"
#include "ogr_spatialref.h"
#include "float.h"
#include "ogr_core.h"
#include "ogr_srs_api.h"

using namespace std;

struct Point {

    Point(double _x, double _y) : x(_x), y(_y) {
    }

    Point() : x(-1), y(-1) {
    }

    double x;
    double y;
};

struct LineList {
    Point sPoint;
    Point ePoint;
};

struct LineList2 {
    Point sPoint;
    Point ePoint;
    long fromPoint;
};

static string shapefile;
static string danglefile;

static void findDanglePoints();
static void findDanglePointsV2();
static bool inLine(Point A, Point B, Point C);
static double distance(Point A, Point B);

double distance(Point A, Point B) {
    return ((A.x - B.x)*(A.x - B.x)) + ((A.y - B.y)*(A.y - B.y));
}

bool inLine(Point A, Point B, Point C) {
    if (distance(A, C) + distance(B, C) == distance(A, B))
        return true;
    return false;
}

void findDanglePointsV2() {
    GDALDatasetH hDSFlow;
    OGRLayerH hLayerFlow;

    hDSFlow = GDALOpenEx(shapefile.c_str(), GDAL_OF_VECTOR, NULL, NULL, NULL);
    if (hDSFlow == NULL) {
        cerr << "ERROR: Failed to open the file: " << shapefile << endl;
        exit(1);
    }

    hLayerFlow = GDALDatasetGetLayer(hDSFlow, 0);

    if (hLayerFlow == NULL) {
        cerr << "ERROR: Failed to open layer of the shapefile: " << shapefile << endl;
        exit(1);
    }

    OGRSpatialReferenceH hSpatialRefFlowLines = OGR_L_GetSpatialRef(hLayerFlow);

    OSRReference(hSpatialRefFlowLines);
    
    vector<LineList2> lineStrings;
    unordered_set<long> toPoints;

    OGR_L_ResetReading(hLayerFlow);

    OGRFeatureDefnH hFDefn = OGR_L_GetLayerDefn(hLayerFlow);
    int indexFromNode = OGR_FD_GetFieldIndex(hFDefn, "FromNode");
    int indexToNode = OGR_FD_GetFieldIndex(hFDefn, "ToNode");

    OGRFeatureH hFeature;

    while ((hFeature = OGR_L_GetNextFeature(hLayerFlow)) != NULL) {
        OGRGeometryH hGeometry;
        hGeometry = OGR_F_GetGeometryRef(hFeature);

        OGRFeatureDefnH hFDefn;
        int iField;
        hFDefn = OGR_L_GetLayerDefn(hLayerFlow);
        int fromPoint = OGR_F_GetFieldAsInteger(hFeature, indexFromNode);
        int toPoint = OGR_F_GetFieldAsInteger(hFeature, indexToNode);

        if (hGeometry != NULL) {
            OGRwkbGeometryType gType = wkbFlatten(OGR_G_GetGeometryType(hGeometry));
            if (gType == wkbLineString) {
                int pointCount = OGR_G_GetPointCount(hGeometry);
                double* xBuffer = (double*) malloc(sizeof (double) * pointCount);
                double* yBuffer = (double*) malloc(sizeof (double) * pointCount);
                int pc = OGR_G_GetPoints(hGeometry, xBuffer, sizeof (double), yBuffer, sizeof (double), NULL, 0);

                Point tmpStart = Point(xBuffer[0], yBuffer[0]);
                Point tmpEnd = Point(xBuffer[pc - 1], yBuffer[pc - 1]);

                LineList2 line;
                line.sPoint = tmpStart;
                line.ePoint = tmpEnd;
                line.fromPoint = fromPoint;
                
                lineStrings.push_back(line);
                
                free(xBuffer);
                free(yBuffer);
                
                auto got = toPoints.find (toPoint);

                if (got == toPoints.end())
                    toPoints.insert(toPoint);
            }
        }

        OGR_F_Destroy(hFeature);
    }
    GDALClose(hDSFlow);

    const char *pszDriverName = "ESRI Shapefile";
    GDALDriver *poDriver;
    poDriver = (GDALDriver*) GDALGetDriverByName(pszDriverName);
    if (poDriver == NULL) {
        cerr << "ERROR: " << pszDriverName << " driver is not available" << endl;
        exit(1);
    }

    GDALDatasetH poDS;

    poDS = GDALCreate(poDriver, danglefile.c_str(), 0, 0, 0, GDT_Unknown, NULL);
    if (poDS == NULL) {
        cerr << "ERROR: Failed to create file: " << danglefile << endl;
        exit(1);
    }

    OGRLayerH hLayerOut;
    hLayerOut = GDALDatasetCreateLayer(poDS, "Dangles", hSpatialRefFlowLines, wkbPoint, NULL);
    if (hLayerOut == NULL) {
        cerr << "ERROR: Failed to create Dangles layer in file: " << danglefile << endl;
        exit(1);
    }
    
    int i, j, k;

    vector<Point> myinlets;

    for (i = 0; i < lineStrings.size(); ++i) {
        LineList2 line = lineStrings[i];

        auto got = toPoints.find (line.fromPoint);

        if (got == toPoints.end())
            myinlets.push_back(line.sPoint);
    }

    for (i = 0; i < myinlets.size(); ++i) {
        Point a = myinlets[i];

        OGRFeatureH hFeature;
        hFeature = OGR_F_Create(OGR_L_GetLayerDefn(hLayerOut));

        OGRGeometryH hPt;
        hPt = OGR_G_CreateGeometry(wkbPoint);
        OGR_G_SetPoint_2D(hPt, 0, a.x, a.y);
        OGR_F_SetGeometry(hFeature, hPt);
        OGR_G_DestroyGeometry(hPt);

        if (OGR_L_CreateFeature(hLayerOut, hFeature) != OGRERR_NONE) {
            cerr << "ERROR: Failed to create feature in file: " << danglefile << endl;
            exit(1);
        }

        OGR_F_Destroy(hFeature);
    }

    GDALClose(poDS);
}

static void findDanglePoints() {
    GDALDatasetH hDSFlow;
    OGRLayerH hLayerFlow;

    hDSFlow = GDALOpenEx(shapefile.c_str(), GDAL_OF_VECTOR, NULL, NULL, NULL);
    if (hDSFlow == NULL) {
        cerr << "ERROR: Failed to open the file: " << shapefile << endl;
        exit(1);
    }

    hLayerFlow = GDALDatasetGetLayer(hDSFlow, 0);

    if (hLayerFlow == NULL) {
        cerr << "ERROR: Failed to open layer of the shapefile: " << shapefile << endl;
        exit(1);
    }

    OGRSpatialReferenceH hSpatialRefFlowLines = OGR_L_GetSpatialRef(hLayerFlow);

    OSRReference(hSpatialRefFlowLines);
    
    vector<LineList> lineStrings;

    OGR_L_ResetReading(hLayerFlow);

    OGRFeatureH hFeature;

    while ((hFeature = OGR_L_GetNextFeature(hLayerFlow)) != NULL) {
        OGRGeometryH hGeometry;
        hGeometry = OGR_F_GetGeometryRef(hFeature);

        OGRFeatureDefnH hFDefn;
        int iField;
        hFDefn = OGR_L_GetLayerDefn(hLayerFlow);
        int streamOrder = OGR_F_GetFieldAsInteger(hFeature, 12);

        if (hGeometry != NULL) {
            OGRwkbGeometryType gType = wkbFlatten(OGR_G_GetGeometryType(hGeometry));
            if (gType == wkbLineString) {
                int pointCount = OGR_G_GetPointCount(hGeometry);
                double* xBuffer = (double*) malloc(sizeof (double) * pointCount);
                double* yBuffer = (double*) malloc(sizeof (double) * pointCount);
                int pc = OGR_G_GetPoints(hGeometry, xBuffer, sizeof (double), yBuffer, sizeof (double), NULL, 0);

                Point tmpStart = Point(xBuffer[0], yBuffer[0]);
                Point tmpEnd = Point(xBuffer[pc - 1], yBuffer[pc - 1]);
                int tmpStreamOrder = streamOrder;

                if (tmpStreamOrder == 1) {
                    LineList line;
                    line.sPoint = tmpStart;
                    line.ePoint = tmpEnd;
                    lineStrings.push_back(line);
                }
                free(xBuffer);
                free(yBuffer);
            }
        }

        OGR_F_Destroy(hFeature);
    }
    GDALClose(hDSFlow);

    const char *pszDriverName = "ESRI Shapefile";
    GDALDriver *poDriver;
    poDriver = (GDALDriver*) GDALGetDriverByName(pszDriverName);
    if (poDriver == NULL) {
        cerr << "ERROR: " << pszDriverName << " driver is not available" << endl;
        exit(1);
    }

    GDALDatasetH poDS;

    poDS = GDALCreate(poDriver, danglefile.c_str(), 0, 0, 0, GDT_Unknown, NULL);
    if (poDS == NULL) {
        cerr << "ERROR: Failed to create file: " << danglefile << endl;
        exit(1);
    }

    OGRLayerH hLayerOut;
    hLayerOut = GDALDatasetCreateLayer(poDS, "Dangles", hSpatialRefFlowLines, wkbPoint, NULL);
    if (hLayerOut == NULL) {
        cerr << "ERROR: Failed to create Dangles layer in file: " << danglefile << endl;
        exit(1);
    }
    
    int i, j, k;

    vector<Point> myinlets;

    for (i = 0; i < lineStrings.size(); ++i) {
        bool addIt = true;
        LineList line = lineStrings[i];

        for (j = 0; j < lineStrings.size(); ++j) {
            if (i == j)
                continue;

            LineList line2 = lineStrings[j];
            if (line.sPoint.x == line2.ePoint.x && line.sPoint.y == line2.ePoint.y) {
                addIt = false;
                break;
            }
        }

        if (addIt)
            myinlets.push_back(line.sPoint);
    }

    for (i = 0; i < myinlets.size(); ++i) {
        Point a = myinlets[i];

        OGRFeatureH hFeature;
        hFeature = OGR_F_Create(OGR_L_GetLayerDefn(hLayerOut));

        OGRGeometryH hPt;
        hPt = OGR_G_CreateGeometry(wkbPoint);
        OGR_G_SetPoint_2D(hPt, 0, a.x, a.y);
        OGR_F_SetGeometry(hFeature, hPt);
        OGR_G_DestroyGeometry(hPt);

        if (OGR_L_CreateFeature(hLayerOut, hFeature) != OGRERR_NONE) {
            cerr << "ERROR: Failed to create feature in file: " << danglefile << endl;
            exit(1);
        }

        OGR_F_Destroy(hFeature);
    }

    GDALClose(poDS);
}

void usage() {
    cout << "INFO: Finds the dangle points on the flow file (-flow)" << endl;
    cout << "INFO: Writes the result into shape file (-dangle)" << endl;
    cout << "USAGE: find_dangles -flow [shape file of flow lines] -dangle [output shape file] (default: dangles.shp)" << endl;
}

/*
 * 
 */
int main(int argc, char** argv) {
    GDALAllRegister();

    shapefile = "";
    danglefile = "";
    
    for (int i = 1; i < argc; ++i) {
        if (string(argv[i]) == "-flow") {
            shapefile = string(argv[++i]);
        } else if (string(argv[i]) == "-dangle") {
            danglefile = string(argv[++i]);
        }
    }
    
    if (danglefile == "") {
        danglefile = "dangles.shp";
    }
    
    if (shapefile == "") {
        usage();
        exit(1);
    }

    findDanglePointsV2();
    return 0;
}


