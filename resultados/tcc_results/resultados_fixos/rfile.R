# Populando os dados
x500k <- c(2.12555,
           2.1311,
           2.13201,
           2.13658,
           2.13707,
           2.13811,
           2.14018,
           2.14048,
           2.14056,
           2.14174,
           2.14634,
           2.14889,
           2.149,
           2.14913,
           2.15523,
           2.16532,
           2.18822,
           2.19965,
           2.23489,
           2.35744)

x1kk <- c(2.12136,
          2.11923,
          2.12969,
          2.1311,
          2.1268,
          2.12804,
          2.26892,
          2.13366,
          2.14378,
          2.13107,
          2.12468,
          2.14703,
          2.13573,
          2.16209,
          2.11641,
          2.22141,
          2.11619,
          2.13689,
          2.1592)

x2kk <- c(2.15524,
          2.13037,
          2.12963,
          2.15296,
          2.12522,
          2.16445,
          2.12727,
          2.14043,
          2.15495,
          2.17314,
          2.18675,
          2.16612,
          2.19602,
          2.19965,
          2.21079,
          2.16586,
          2.16448,
          2.1661,
          2.17979,
          2.16092)

dados <- list(
  x500k,
  x1kk,
  x2kk
)

grupos <- c(
  "500k",
  "1kk",
  "2kk"
)

#Realizando o teste de Kruskal-Wallis
resultado_teste <- kruskal.test(dados, grupos)


# Exibindo os resultados
print(resultado_teste)


operation <- "Alteração"
estrategia <- "Class Table Karwin"
plot(density(x500k))
plot(density(x1kk))
plot(density(x2kk))
boxplot(dados, names=grupos, main=paste(estrategia,operation, sep=" - "), xlab="Grupos", ylab="Valores", col="lightblue")

