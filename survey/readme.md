# Survey Plans

## Sumary of the paper
"If HTTPS Were Secure, I Wouldn't Need 2FA" - End User and Administrator Mental Models of HTTPS.
Krombholz et. al performed a qualitative study of the mental models of HTTPS held by end users and administrators.
The work disclosed that some administrators have misconceptions about the security benefits of HTTPS.
The survey showed that they had incomplete knowledge of HTTPS.
Furthermore, administrators sometimes misunderstand the interplay of HTTPS protocol components.
We replicate parts of the survey carried out in the work by Krombholz et. al for SRI.
We show that ....

## Goal
* Test web developers' understanding of the SRI recommendation
* Verify if the behaviour correlates with the large scale analysis

## Participants
* Web developers
* CDN operators

## Screening Questions
* Age
* Gender
* Profession
* Highest level of completed education
* Recent professional status

## Short Task
* Provide a simple web page in any language that contains images, scripts or other downloads
* Ask user to add SRI
**We could find the sites that are regularly visited by the admins as well as the way they use the resources**

## Other Possible Tasks
* Ask the participant to draw what happens with included scripts when user accesses the web page
* Ask user to think of attacker models in this respect and draw them
* Ask the user to describe what happen if a snippet (possibly malformed) is included in the webpage

## Expert Questions
* Are you a web developer/administrator? If yes since when: ...
* Do you have an IT security background? If yes, please specify: ...
* Attachment (Freelance or Attached), for how long?
  * If Attached, how big is the company that you are working for?
* Do you mostly build Single Page Applications (SPA) or Multi Page Applications (MPA)
* Does security play a role in your work life? Likert scale from 1(Agree) to 7(Disagree)
* How do you find out about new web security features?
* How do you find new implementations of new web security features?
* Do you copy and paste new implementations of web security features directly from online forums? if no, specify what kind of changes that you make
* I am often providing code snippets to help other web developers. Likert scale from 1(Agree) to 7(Disagree)
* Do you create libraries for other developer's to use?
* What type of development stack do you use?
* What type of build tools do you use?
* Does your web application rely on Content Delivery Networks?
* Do you build so-called "microservices"?
* Does your applications span across several subdomains (e.g. www.myapp.com; cdn.myapp.com)

## Interview Protocol
* Have you heard about SRI? If yes, since when: ...
* Have you used SRI? If yes, since when: ...
* Have you encountered the integrity attribute in a web page? Yes/No
  * If yes, in which HTML element have you encountered this attribute?
* Have you copy pasted a snippet that specifies the integrity attribute in a web page? Yes/No
  * If yes, where did this snippet came from?
* Have you configured a build tool to automatically generate SRI on your behalf? Yes/No
  * If yes, which build tool did you use?.
* Explain the purpose of the integrity attribute in your own words.
* Among the following values for the integrity attribute, which one can prevent the inclusion of the subresource?
  * sha256-kI8CAhrL/OzNAQDYCcWodzP+A17kIY9u6iUXn9p32Q4= (Y)
  * sha256-kI8CAhrL/OzNAQDYCcWodzP+A17kIY9u6iUXn9p32Q4 (Y)
  * sha512-Dj51I0q8aPQ3ioaz9LMqGYujAYRbDNblAQbodDRXAMxmY6hsHqEl3F6SvhfJj5oPhcqdX1ldsgEvfMNXGUXBIw== (Y)
  * sha224-UvG/CT9LdYhyYDXBdsDNtDds/qU4GfE5Wsnm7A== (Y)
  * sha384-EJu2tbbVVHwc4Dx6i9fY+AwcsJV/UMT3/aBGkgeZF+T5ytUrh489gjThoXCxVLct (Y)
  * sha256-kI8CAhrL\OzNAQDYCcWodzP+A17kIY9u6iUXn9p32Q4= (N)
  * md5-2Oj8otwPiW/Xy0ywAxuiSQ== (N)
  * sha256kI8CAhrL/OzNAQDYCcWodzP+A17kIY9u6iUXn9p32Q4= (N)
* Can SRI be used in place of HTTPS? Why?
* Can HTTPS be used in place of SRI? Why?
* Should SRI be used with HTTPS? Why?
* Should SRI be used with HTTP? Why?
* Have you already integrated a third-party library on a website?
  * If yes, how did you do it?
  * What were your security concerns in doing so?
  * Would you use SRI with other kind of subresources?
* Would you like to use the integrity attribute with other subresources (img, video, etc.)

