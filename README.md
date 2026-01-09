# falali-api



## Getting started
nohup java -Duser.timezone=Asia/Shanghai -jar falali-api-0.0.1-SNAPSHOT.jar > logs/app.log 2>&1 &
grep "下单花费" nohup.out | awk -F "下单花费:" '{if($2+0 > 10000) print $0}'
redis-cli -a dajusheng@20241201. --rdb dump.rdb
To make it easy for you to get started with GitLab, here's a list of recommended next steps.
加拿大28采用加拿大彩票公司BCLC的Keno游戏数据【BCLC官网：lotto.bclc.com】
开奖频率：当加拿大夏令时，开奖时间为北京时间每天20:00至第二天19:00；当加拿大冬令时，开奖时间为北京时间每天20:33:00至第二天19:57:30；每3.5分一期,每天402期。
加拿大28是根据什么开出的？
加拿大彩票公司BCLC彩每期结果共开出20个数字。加拿大28将这20个号码按照由小到大的顺序依次排列:
取其第2/5/8/11/14/17位开出号码相加，和值的末位数作为加拿大28开出第一个数值，取其第3/6/9/12/15/18位开出号码相加，和值的末位数作为加拿大开出第二个数值，取其第4/7/10/13/16/19位开出号码相加，和值的末位数作为加拿大28开出第三个数值，三个数值相加即为加拿大28总和值最终的结果。
加拿大28玩法说明
例如：加拿大BCLC第"1749110"期数据从小到大排序 7,8,14,16,17,22,26,34,39,41,42,48,54,58,63,64,69,72,73,79
第一区[第2/5/8/11/14/17位数字] 8,17,34,42,58,69
计算：8+17+34+42+58+69= 228
结果为：8
第二区[第3/6/9/12/15/18位数字] 14,22,39,48,63,72
计算：14+22+39+48+63+72= 258
结果为：8
第三区[第4/7/10/13/16/19位数字] 16,26,41,54,64,73
计算：16+26+41+54+64+73= 274
结果为：4
最终游戏结果为：8+8+4=20

主势盘：
    和值：根据游戏开奖结果的和值，如8+8+4=20就是20位置
单球1-3：
    第一球的号码结果是开奖加法中的第一位，如8+8+4=20就是8，双面就是大和双（8属于大和双）
    第二球的号码结果是开奖加法中的第二位，如8+8+4=20就是8，双面就是大和双（8属于大和双）
    第三球的号码结果是开奖加法中的第三位，如8+8+4=20就是4，双面就是小和双（4属于小和双）
二字定位：
    佰拾定位oox：共100个位置，根据游戏结果加法中的第一位和第二位，如8+8+4=20就是88位置中
    佰个定位oxo：共100个位置，根据游戏结果加法中的第一位和第三位，如8+8+4=20就是84位置中
    拾个定位xoo：共100个位置，根据游戏结果加法中的第二位和第三位，如8+8+4=20就是84位置中
三字定位：
    共1000个位置，根据游戏结果加法中的第一，二，三位结合，，如8+8+4=20就是884位置中
两面盘：
    第一球：只有大小单双，根据游戏结果加法中的第一位,如8+8+4=20就是大和双
    第二球：只有大小单双，根据游戏结果加法中的第二位,如8+8+4=20就是大和双
    第三球：只有大小单双，根据游戏结果加法中的第三位,如8+8+4=20就是小和双
    龙：开奖的第一位号码大于第三位号码，如第一球开出9，第三球开出2，投注【龙】视为中奖，如出和局为打和
    虎：开奖的第一位号码小于第三位号码，如第一位开出3，第三球开出6，投注【虎】视为中奖，如出和局为打和
    和：开奖的第一位号码和第三位号码相同，投注【和】视为中奖，其余情形视为不中奖
    总和大小：当期开奖结果三个彩球号码的总和，总和值0~13为【总和小】:14~27为【总和大】
    总和单双：当期开奖结果三个彩球号码的总和，总和值是单数为【总和单】;总和值是双数为【总和双】

Already a pro? Just edit this README.md and make it your own. Want to make it easy? [Use the template at the bottom](#editing-this-readme)!

## Add your files

- [ ] [Create](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#create-a-file) or [upload](https://docs.gitlab.com/ee/user/project/repository/web_editor.html#upload-a-file) files
- [ ] [Add files using the command line](https://docs.gitlab.com/ee/gitlab-basics/add-file.html#add-a-file-using-the-command-line) or push an existing Git repository with the following command:

```
cd existing_repo
git remote add origin http://gitlab/falali/falali-api.git
git branch -M main
git push -uf origin main
```

## Integrate with your tools

- [ ] [Set up project integrations](http://gitlab/falali/falali-api/-/settings/integrations)

## Collaborate with your team

- [ ] [Invite team members and collaborators](https://docs.gitlab.com/ee/user/project/members/)
- [ ] [Create a new merge request](https://docs.gitlab.com/ee/user/project/merge_requests/creating_merge_requests.html)
- [ ] [Automatically close issues from merge requests](https://docs.gitlab.com/ee/user/project/issues/managing_issues.html#closing-issues-automatically)
- [ ] [Enable merge request approvals](https://docs.gitlab.com/ee/user/project/merge_requests/approvals/)
- [ ] [Set auto-merge](https://docs.gitlab.com/ee/user/project/merge_requests/merge_when_pipeline_succeeds.html)

## Test and Deploy

Use the built-in continuous integration in GitLab.

- [ ] [Get started with GitLab CI/CD](https://docs.gitlab.com/ee/ci/quick_start/index.html)
- [ ] [Analyze your code for known vulnerabilities with Static Application Security Testing (SAST)](https://docs.gitlab.com/ee/user/application_security/sast/)
- [ ] [Deploy to Kubernetes, Amazon EC2, or Amazon ECS using Auto Deploy](https://docs.gitlab.com/ee/topics/autodevops/requirements.html)
- [ ] [Use pull-based deployments for improved Kubernetes management](https://docs.gitlab.com/ee/user/clusters/agent/)
- [ ] [Set up protected environments](https://docs.gitlab.com/ee/ci/environments/protected_environments.html)

***

# Editing this README

When you're ready to make this README your own, just edit this file and use the handy template below (or feel free to structure it however you want - this is just a starting point!). Thanks to [makeareadme.com](https://www.makeareadme.com/) for this template.

## Suggestions for a good README

Every project is different, so consider which of these sections apply to yours. The sections used in the template are suggestions for most open source projects. Also keep in mind that while a README can be too long and detailed, too long is better than too short. If you think your README is too long, consider utilizing another form of documentation rather than cutting out information.

## Name
Choose a self-explaining name for your project.

## Description
Let people know what your project can do specifically. Provide context and add a link to any reference visitors might be unfamiliar with. A list of Features or a Background subsection can also be added here. If there are alternatives to your project, this is a good place to list differentiating factors.

## Badges
On some READMEs, you may see small images that convey metadata, such as whether or not all the tests are passing for the project. You can use Shields to add some to your README. Many services also have instructions for adding a badge.

## Visuals
Depending on what you are making, it can be a good idea to include screenshots or even a video (you'll frequently see GIFs rather than actual videos). Tools like ttygif can help, but check out Asciinema for a more sophisticated method.

## Installation
Within a particular ecosystem, there may be a common way of installing things, such as using Yarn, NuGet, or Homebrew. However, consider the possibility that whoever is reading your README is a novice and would like more guidance. Listing specific steps helps remove ambiguity and gets people to using your project as quickly as possible. If it only runs in a specific context like a particular programming language version or operating system or has dependencies that have to be installed manually, also add a Requirements subsection.

## Usage
Use examples liberally, and show the expected output if you can. It's helpful to have inline the smallest example of usage that you can demonstrate, while providing links to more sophisticated examples if they are too long to reasonably include in the README.

## Support
Tell people where they can go to for help. It can be any combination of an issue tracker, a chat room, an email address, etc.

## Roadmap
If you have ideas for releases in the future, it is a good idea to list them in the README.

## Contributing
State if you are open to contributions and what your requirements are for accepting them.

For people who want to make changes to your project, it's helpful to have some documentation on how to get started. Perhaps there is a script that they should run or some environment variables that they need to set. Make these steps explicit. These instructions could also be useful to your future self.

You can also document commands to lint the code or run tests. These steps help to ensure high code quality and reduce the likelihood that the changes inadvertently break something. Having instructions for running tests is especially helpful if it requires external setup, such as starting a Selenium server for testing in a browser.

## Authors and acknowledgment
Show your appreciation to those who have contributed to the project.

## License
For open source projects, say how it is licensed.

## Project status
If you have run out of energy or time for your project, put a note at the top of the README saying that development has slowed down or stopped completely. Someone may choose to fork your project or volunteer to step in as a maintainer or owner, allowing your project to keep going. You can also make an explicit request for maintainers.
