#!/bin/bash
cbt -instance dev -project emulator createtable weather_balloons
cbt -instance dev -project emulator createfamily weather_balloons measurements
