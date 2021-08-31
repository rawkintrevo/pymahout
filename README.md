
## Here thar be dragons

So the first mess to get through is trying to get all the components to play nice. To do that we need to get Java, Py4j, Spark, Mahout, et al. jiving. 

See the Dockerfile in environment. Make that docker file- then run the file in tester.py- its sets it up and lets you make some random matricies (incore). 

First step- i just wanted to get a first push up. 

1. Build mahout- copy dependency reduced jar as well as core/hsdf/spark jars to env/ 
2. download apache spark 2.4.7 tgz also to env/
3. build docker file, something like `docker build -t rawkintrevo/pymahout .`
4. `docker run -it rawkintrevo/pymahout python`
5. profit

Good Mahouting,

tg
