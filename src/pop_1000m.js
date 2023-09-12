#!/usr/bin/env node

//See https://observablehq.com/d/62acd402b96476b9
//https://uwdata.github.io/arquero/api/

const aq = require("arquero");
const { writeFileSync } = require('fs');
const { execSync } = require("child_process");


const prepareCSV = async (year) => {

    console.log("Load CSV file " + year)

    //load files
    let dt = await aq.loadCSV("./input/pop_1000m/NOR01000M_POP_" + year + ".csv", { delimiter: ";" });

    //rename population column
    //dt = dt.rename({ "pop_tot": "y" + y })

    console.log(dt.columnNames())

    console.log("Save")

    const csv = aq.table(out).toCSV()
    writeFileSync("./input/out_pop_1000m_" + year + ".csv", csv);

}

prepareCSV(2019)




const tiling = (year) => {

    //gothrough several aggregation levels
    for (let a of [1, 2, 5, 10, 20, 50]) {

        console.log("Tiling " + year + " to " + a + "m")

        execSync(
            'gridtiler -i ./input/pop_1000m/NOR1000m.csv -r 250 -c 3035 -x 1900000 -y 6400000 -p "return {x:+c.SSBID0250M.substring(0,7), y:+c.SSBID0250M.substring(7,14)};" -m "delete c.SSBID0250M" -a ' + a + ' -o ./out/popcsv/' + (a * 250) + 'm/ -e csv -t 32'
            , { stdio: 'inherit' }
        );
    }
}

//tiling()
