FROM microsoft/dotnet:latest
ARG SOURCEDIR
WORKDIR /brocker
COPY ${SOURCEDIR} .
ENTRYPOINT ["dotnet", "CandlesWriter.Broker.dll"]
