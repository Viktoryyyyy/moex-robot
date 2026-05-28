class ArtifactContract:
    def __init__(self, artifact_id, artifact_class, producer, consumer, artifact_format, schema_version):
        values = [artifact_id, artifact_class, producer, consumer, artifact_format, schema_version]
        if any(not isinstance(value, str) or not value.strip() for value in values):
            raise ValueError("artifact contract fields must be non-empty strings")
        self.artifact_id = artifact_id
        self.artifact_class = artifact_class
        self.producer = producer
        self.consumer = consumer
        self.format = artifact_format
        self.schema_version = schema_version
