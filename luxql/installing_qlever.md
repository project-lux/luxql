

## Setup the machine

apt-get update && apt-get install -y software-properties-common wget && add-apt-repository -y ppa:mhier/libboost-latest
wget https://apt.kitware.com/kitware-archive.sh && chmod +x kitware-archive.sh &&./kitware-archive.sh
apt-get install -y build-essential cmake libicu-dev tzdata pkg-config uuid-runtime uuid-dev git libjemalloc-dev ninja-build libzstd-dev libssl-dev

apt-get install -y postgresql postgresql-contrib

### For Ubuntu 22:
apt-get install -y libboost1.81-dev libboost-program-options1.81-dev libboost-iostreams1.81-dev libboost-url1.81-dev

### For Ubuntu 24:
apt-get install -y libboost1.83-dev libboost-program-options1.83-dev libboost-iostreams1.83-dev libboost-url1.83-dev

mkdir /data-io2/data
mkdir /data-io2/qlever
mkdir /data-io2/postgres
chown ubuntu:ubuntu /data-io2/*


## Setup data and postgresql


sudo su - postgres
createuser ubuntu -d -r -s
exit
createdb ubuntu
psql # this should work now

sudo su -
cd /var/lib/postgresql
mv 16 /data-io2/postgres
ln -s /data-io2/postgres/16 16
service postgresql restart
exit

### Load the data

// scp the data from a machine that has it





## Building Qlever

git clone --recursive -j 8 https://github.com/project-lux/qlever.git
cd qlever
mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=Release .. && make -j
(wait)
cd ../..
mkdir bin
cp qlever/build/IndexBuilderMain qlever/build/ServerMain qlever/build/PrintIndexVersionMain qlever/build/VocabularyMergerMain qlever/build/spatialjoin bin/

// scp the triples from a machine that has them

ulimit -Sn 500000 && zcat lux_*.nt.gz | ../code/bin/IndexBuilderMain -i lux -s lux.settings.json -F nt -f - -p true --text-words-from-literals --stxxl-memory 60G





## Building Indexes

cd ..
git clone https://github.com/project-lux/luxql.git







## Set up middletier

cd /data-io2
python3 -m venv ENV
source ENV/bin/activate
pip install -r luxql/requirements.txt