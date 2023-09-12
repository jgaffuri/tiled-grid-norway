#!/usr/bin/env node

//See https://observablehq.com/d/62acd402b96476b9
//https://uwdata.github.io/arquero/api/

const aq = require("arquero");
const { writeFileSync } = require('fs');
const { execSync } = require("child_process");


const prepareCSV = async (year) => {

    console.log("Load CSV file " + year)

    //load files
    let dt = await aq.loadCSV("./input/pop_1000m/NOR1000M_POP_" + year + ".csv", { delimiter: ";" });

    //rename population column
    //dt = dt.rename({ "pop_tot": "y" + y })

    dt = dt.derive({
        ta: aq.escape(d => +(d.pop_tot * +d.pop_ave2.replace(",", ".")).toFixed(1))
    });

    //keep only relevant columns
    dt = dt.select("SSBID1000M", "pop_tot", "pop_mal", "pop_fem", "ta")

    //rename columns
    dt = dt.rename({ "pop_tot": "p", "pop_mal": "m", "pop_fem": "f" })

    //console.log(dt.print());

    //console.log(dt.columnNames())

    console.log("Save")
    const csv = aq.table(dt).toCSV()
    writeFileSync("./input/out_pop_1000m_" + year + ".csv", csv);
}

//prepare
for (let y = 2001; y <= 2019; y++)
    prepareCSV(y)




const tiling = (year) => {

    //gothrough several aggregation levels
    for (let a of [1, 2, 5, 10, 20, 50, 100]) {

        console.log("Tiling " + year + " to " + a + "m")

        execSync(
            'gridtiler -i ./input/out_pop_1000m_' + year + '.csv -r 1000 -c 25833 -x 1900000 -y 6400000 -p "return {x:+c.SSBID1000M.substring(0,7), y:+c.SSBID1000M.substring(7,14)};" -m "delete c.SSBID1000M" -a ' + a + ' -o ./out/pop' + year + 'csv/' + (a * 1000) + 'm/ -e csv -t 128'
            , { stdio: 'inherit' }
        );
    }
}

//tiling(2019)
