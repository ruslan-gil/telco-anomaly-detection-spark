### Plots antenna patterns produced by running the test suite

pdf("plot.pdf", width=4, height=4)
cat(par("mar"))
par(mar=c(3.1,4.1,4.1,2.1))
x = read.csv("/home/dzhuribeda/dev/telco-anomaly-detection-spark/beam-1.csv")
y = read.csv("/home/dzhuribeda/dev/telco-anomaly-detection-spark/beam-2.csv")
z = read.csv("/home/dzhuribeda/dev/telco-anomaly-detection-spark/beam-3.csv")

plot(py~px,y, type='l', xlim=c(-1,1), ylim=c(-1,1))
lines(py~px,x,col='green')
lines(py~px,z,col='red')
dev.off()
