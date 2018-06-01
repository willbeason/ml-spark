setwd("C:/Users/william/code/ml-spark")

data <- read.csv("data/all-crimes-locations-s.csv")

#data <- data[data[,1] >= 41.70206,]
#data <- data[data[,1] <= 41.97737,]

data <- data[data[,2] >= -91,]
data <- data[,c(2,1)]
#data <- data[data[,2] <= -87.57526,]

data2 <- as.matrix(data)

library(hexbin)

h30 <- hexbin(data2, xbins=30)
plot(h30)

h100 <- hexbin(data2, xbins=100)
plot(h100)

h1000 <- hexbin(data2, xbins=1000)

png(filename = "heatmap1k.png", width=3000,height=3000)
plot(h1000)
dev.off()
