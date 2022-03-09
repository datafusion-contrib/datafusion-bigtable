#!/bin/bash

cbt -instance dev -project emulator  createtable weather_balloons
cbt -instance dev -project emulator  createfamily weather_balloons measurements
cbt -instance dev -project emulator set weather_balloons us-west2#3698#2021-03-05-1200 measurements:pressure=94558@1614945605100000
cbt -instance dev -project emulator set weather_balloons us-west2#3698#2021-03-05-1201 measurements:pressure=94122@1614945665200000
cbt -instance dev -project emulator set weather_balloons us-west2#3698#2021-03-05-1202 measurements:pressure=95992@1614945725300000
cbt -instance dev -project emulator set weather_balloons us-west2#3698#2021-03-05-1203 measurements:pressure=96025@1614945785400000
cbt -instance dev -project emulator set weather_balloons us-west2#3698#2021-03-05-1204 measurements:pressure=96021@1614945845500000
