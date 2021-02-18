var program = require('commander');
var pkg = require('../package.json');

program.parse(process.argv);

if(program.args[0] == "convert"){
    program
        .command('convert [db] [design] [view] ', 'Emit data results .CSV file.')
        .version(pkg.version)
        .usage('[options] <db> <design> <view>')
        .parse(process.argv);
}

if(program.args[0] == "assemble"){
    program
        .command('assemble [machination] ', 'Instruct Coeus to assemble one of his Machinations.')
        .version(pkg.version)
        .usage('[options] <machination>')
        .parse(process.argv);
}

if(program.args[0] == "model"){
    program
        .command('model [curiosity]', 'Instruct Coeus to model one of his Curiosities.')
        .version(pkg.version)
        .usage('[options] <curiosity>')
        .option('-p, --phase [value]', 'Set the Phase at which to begin modelling the requested Curiosity.')
        .parse(process.argv);
}

if(program.args[0] == "mine"){
    program
        .command('mine [file]', 'Instruct Coeus to mine the data from a dBase file.')
        .version(pkg.version)
        .usage('[options] <file>')
        .option('-s, --start-recno [value]', 'Set the depth at which to start mining.')
        .option('-b, --brake [value]', 'Set the brake speed (Full Batch)< 0 -- 6 >(Single Record).')
        .parse(process.argv);
}