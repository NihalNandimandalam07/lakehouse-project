import aws_cdk as core
import aws_cdk.assertions as assertions

from lake_house_project.lake_house_project_stack import LakeHouseProjectStack

# example tests. To run these tests, uncomment this file along with the example
# resource in lake_house_project/lake_house_project_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = LakeHouseProjectStack(app, "lake-house-project")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
