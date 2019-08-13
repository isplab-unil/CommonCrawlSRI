# Quantitative Survey

## Goal
* Test web developers' understanding of the SRI recommendation
* Verify if the behaviour correlates with the large scale analysis

## Research Questions
* Are developers aware of SRI?
* To what extent are developers using SRI in their web applications?
* How are developers using SRI in their web applications? According to specification  or not?

## Participants
* Web developers
* CDN operators

## Screening Questions
* Age
* Gender
* Profession
* Highest level of completed education
* Recent professional status

## Expert Questions
* Are you a web developer/administrator? If yes since when: ...
* Do you have an IT security background? If yes, please specify: ...
* Attachment (Freelance or Attached), for how long?
  * If Attached, how big is the company that you are working for?
* Do you mostly build Single Page Applications (SPA) or Multi Page Applications (MPA)
* Does security play a role in your work life? Likert scale from 1(Agree) to 7(Disagree)
* I am often providing code snippets to help other web developers. Likert scale from 1(Agree) to 7(Disagree)
* What source of information do you use to find out about new web security features? **euronews, Hacker news e.t.c**
* How do you find reference implementations of new web security features? **Stackoverflow, Stackexchange e.t.c**
* Do you copy and paste reference implementations of web security features directly from online forums? Yes/No 
  * If no, specify what kind of changes that you make
* Do you create libraries for other developer's to use? Yes/No
* What type of development stack do you use?
* What type of build tools do you use?
* Does your web application rely on Content Delivery Networks? Yes/No
* Do you build so-called "microservices"? Yes/No
* Does your applications span across several subdomains (e.g. www.myapp.com; cdn.myapp.com) Yes/No

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
* SRI and HTTPS perform the same function and can be used interchangeably. Yes/No
  * If yes, why?
  * If no, why?
* Should SRI be used with HTTPS? Why?
* Should SRI be used with HTTP? Why?
* Have you already integrated a third-party library on a website?
  * If yes, how did you do it?
  * What were your security concerns in doing so?
  * Would you use SRI with other kind of subresources?
* Would you like to use the integrity attribute with other subresources (img, video, etc.)
