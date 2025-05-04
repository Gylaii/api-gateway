FROM gradle:jdk17
WORKDIR /app
COPY . .
EXPOSE 8080
CMD ["gradle", "run", "--no-daemon"]
