import os
from django.core.management.base import BaseCommand, CommandError

STUB_CONTENT = """import os
import sys
from reproq_django.executor import execute

if __name__ == "__main__":
    # Custom environment setup here (e.g., PYTHONPATH, secrets)
    # os.environ["MY_SECRET"] = "..."
    
    execute()
"""

class Command(BaseCommand):
    help = "Generates a project-local executor stub"

    def add_arguments(self, parser):
        parser.add_argument("--path", type=str, default="executor_stub.py", help="Path to write the stub to")
        parser.add_argument("--force", action="store_true", help="Overwrite existing file")

    def handle(self, *args, **options):
        path = options["path"]
        if os.path.exists(path) and not options["force"]:
            raise CommandError(f"File {path} already exists. Use --force to overwrite.")
        
        with open(path, "w") as f:
            f.write(STUB_CONTENT)
        
        self.stdout.write(self.style.SUCCESS(f"Generated executor stub at {path}"))
        self.stdout.write(f"You can now use this as your worker executor: python {path} --payload-stdin")
