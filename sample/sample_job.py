from dbx_config.jobs.sample.common import Job


class SampleJob(Job):

    def launch(self):
        self.logger.info("Launching sample job")

        listing = self.dbutils.fs.ls("dbfs:/")

        for l in listing:
            self.logger.info(f"DBFS directory: {l}")

        df = self.spark.range(0, 1000)

        df.write.format("delta").mode("overwrite").save(
            "dbfs:/dbx/tmp/test/dbx_config"
        )

        self.logger.info("Sample job finished!")


if __name__ == "__main__":
    job = SampleJob()
    job.launch()
