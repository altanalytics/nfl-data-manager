FROM public.ecr.aws/lambda/provided:al2023-arm64

# Setting home env variable
ENV HOME=/root

RUN dnf update -y

# Install necessary system dependencies
RUN dnf -y install R awscli wget tar \
    poppler-cpp-devel libpsl-devel perl-core \
    libcurl-devel ca-certificates glibc-langpack-en

# Install locale package for en_US.UTF-8 support (skip locale generation)
RUN dnf -y install glibc-langpack-en

# Set the locale environment variables without running localedef
ENV LANG=en_US.UTF-8
ENV LANGUAGE=en_US:en
ENV LC_ALL=en_US.UTF-8

# System requirements for R packages jpeg/png for tableHTML, xml2 for paws
RUN dnf -y install openssl-devel libxml2-devel libjpeg-turbo-devel libpng-devel
RUN dnf -y install bzip2 xz lz4 zstd brotli # for Arrow

# Install R packages
RUN Rscript -e "install.packages(c('lambdr','jsonlite','httr','nflfastR','lubirdate'), repos = 'https://cloud.r-project.org/')"
RUN Rscript -e "install.packages(c('tableHTML','readr','aws.s3','dplyr','tidyr','rvest'), repos = 'https://cloud.r-project.org/')"

# Copy R files to Lambda task root
COPY *.R ${LAMBDA_TASK_ROOT}/
RUN chmod 755 -R ${LAMBDA_TASK_ROOT}/

# Create bootstrap script that calls the correct R file
RUN printf '#!/bin/sh\nRscript ${LAMBDA_TASK_ROOT}/nfl_lambda_func.R' > /var/runtime/bootstrap \
  && chmod +x /var/runtime/bootstrap

CMD ["lambda_manager"]

