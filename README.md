# Курс Введение в Data Engineering: дата пайплайны

Репозиторий содержит материалы для [моего курса по построению дата пайплайнов на Luigi](https://startdatajourney.com/ru/course/luigi-data-pipelines?utm_source=github&utm_medium=repository&utm_campaign=luigi-course-materials) 

# build image
docker build -t luigi .

# run image
docker run -d -p 8082:8082 --name luigi_container --rm luigi
