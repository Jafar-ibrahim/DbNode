FROM openjdk:17

WORKDIR /app

COPY . .

EXPOSE 9000

CMD ["java", "-jar", "target/DbNode-1.0.jar"]