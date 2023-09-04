#!/usr/bin/env node

//See https://observablehq.com/d/62acd402b96476b9
//https://uwdata.github.io/arquero/api/

//import {aq, op} from "arquero"
const aq = require("arquero");
const fs = require('fs');

(async () => {

    console.log("Load CSV files")

    //load all files, one by one
    const tables = []
    for (let y = 2001; y <= 2022; y++) {
        let dt = await aq.loadCSV("./input/NOR0250M_POP_" + y + ".csv", { delimiter: ";" });
        //rename population column
        dt = dt.rename({ "pop_tot": "pop_" + y })
        tables.push(dt)
    }

    console.log("Joins")

    let out = tables[0]
    for (let i = 1; i <= 15; i++) {
        out = out.join_full(tables[i], "SSBID0250M")
    }
    console.log(out.columnNames())

    console.log("Save")
    const csv = aq.table(out).toCSV()
    fs.writeFileSync("./input/out.csv", csv);

})()
