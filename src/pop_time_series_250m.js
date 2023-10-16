#!/usr/bin/env node

//See https://observablehq.com/d/62acd402b96476b9
//https://uwdata.github.io/arquero/api/

const aq = require("arquero");
const { writeFileSync } = require('fs');
const { execSync } = require("child_process");



const prepareCSV = async () => {

    console.log("Load CSV files")

    //load all files, one by one
    const tables = []
    for (let y = 2001; y <= 2022; y++) {
        let dt = await aq.loadCSV("./input/pop_250m/NOR0250M_POP_" + y + ".csv", { delimiter: ";" });
        //rename population column
        dt = dt.rename({ "pop_tot": "y" + y })
        tables.push(dt)
    }


    console.log("Joins")

    let out = tables[0]
    for (let i = 1; i <= 21; i++) {
        out = out.join_full(tables[i], "SSBID0250M")
    }
    console.log(out.columnNames())

    console.log("Save")

    const csv = aq.table(out).toCSV()
    writeFileSync("./input/out_pop_250m.csv", csv);

}

//prepareCSV()



const tiling = (enc, t) => {

    //gothrough several aggregation levels
    for (let a of [1, 2, 4, 8, 20, 40, 80, 200, 400]) {

        console.log("Tiling " + enc + " to " + (a * 250) + "m")

        execSync(
            'gridtiler -i ./input/out_pop_250m.csv -r 250 -c 25833 -x 1900000 -y 6400000 -p "return {x:+c.SSBID0250M.substring(0,7), y:+c.SSBID0250M.substring(7,14)};" -m "delete c.SSBID0250M" -a ' + a + ' -o ./out/popTS' + enc + '/' + (a * 250) + 'm/ -e ' + enc + ' -t ' + t
            , { stdio: 'inherit' }
        );
    }
}

tiling("csv", 32)
tiling("parquet", 128)
