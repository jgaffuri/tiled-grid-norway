#!/usr/bin/env node

//See https://observablehq.com/d/62acd402b96476b9
//https://uwdata.github.io/arquero/api/

const aq = require("arquero");
const { writeFileSync } = require('fs');
const { execSync } = require("child_process");


const prepareCSV = async (year) => {

    console.log("Load CSV file " + year)
    let dt = await aq.loadCSV("./input/pop_250m/NOR0250M_POP_" + year + ".csv", { delimiter: ";" });

    //rename columns
    dt = dt.rename({ "pop_tot": "p" })

    console.log("Save")
    const csv = aq.table(dt).toCSV()
    writeFileSync("./input/out_pop_250m_" + year + ".csv", csv);
}

//prepare
//for (let y = 2001; y <= 2022; y++) prepareCSV(y)




const tiling = (year) => {

    //gothrough several aggregation levels
    for (let a of year < 2020 ? [1, 2] : [1, 2, 4, 8, 20, 40, 80, 200, 400]) {

        console.log("Tiling " + year + " to " + a + "m")

        execSync(
            'gridtiler -i ./input/out_pop_250m_' + year + '.csv -r 250 -c 25833 -x 1900000 -y 6400000 -p "return {x:+c.SSBID1000M.substring(0,7), y:+c.SSBID1000M.substring(7,14)};" -m "delete c.SSBID1000M" -a ' + a + ' -o ./out/pop' + year + 'csv/' + (a * 250) + 'm/ -e csv -t 128'
            , { stdio: 'inherit' }
        );
    }
}

//tiling
for (let y = 2001; y <= 2022; y++) tiling(y)
