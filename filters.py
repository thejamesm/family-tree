from jinja2 import pass_eval_context
from markupsafe import Markup, escape

@pass_eval_context
def parse_notes(eval_ctx, value):
    br = '<br>\n'
    ul = '<ul>\n'
    lu = '</ul>\n'
    li = '<li>'
    il = '</li>\n'

    if eval_ctx.autoescape:
        value = escape(value)
        br = Markup(br)
        ul = Markup(ul)
        lu = Markup(lu)
        li = Markup(li)
        il = Markup(il)

    within_list = False
    output = []
    for line in value.splitlines():
        if line and line[0] == '-':
            if not within_list:
                output.append(ul)
                within_list = True
            output.append(li + line[1:].strip() + il)
        else:
            if within_list:
                output.append(lu)
                within_list = False
            output.append(line.strip() + br)

    if within_list:
        # Close open list
        output.append(lu)
    else:
        # Remove trailing <br>
        output[-1] = value.splitlines()[-1]

    result = ''.join(output)
    return Markup(result) if eval_ctx.autoescape else result