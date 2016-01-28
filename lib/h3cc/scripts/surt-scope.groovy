//Groovy
rule = appCtx.getBean("scope").rules.find{ rule ->
  rule.class == org.archive.modules.deciderules.surt.SurtPrefixedDecideRule &&
  rule.decision == org.archive.modules.deciderules.DecideResult.ACCEPT
}
 
//dump the list of surts excluded to check results
rule.surtPrefixes.each{ rawOut.println(it) }